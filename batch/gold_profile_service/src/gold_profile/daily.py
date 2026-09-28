"""하루치를 순서대로 돌린다: 전력 Silver -> 사용 요약 -> Gold -> 아웃박스 발행.

Silver도 Gold도 일회성 배치라, 지금까지는 사람이 네 명령을 순서대로 넣어야
원본 적재부터 모니터링 프로필까지 흘렀다. 한 단계가 실패하면 그 뒤는 조용히
돌지 않았고, 어디서 멈췄는지는 로그를 뒤져야 알 수 있었다. 여기서 그 순서와
단계별 재시도를 한 실행에 묶는다.

창 정렬이 이 파이프라인의 핵심이다. Gold는 프로필 창(기본 28일)의 모든 날짜가
**같은 세션 스냅샷에서 만들어졌을 것**을 요구한다(:mod:`gold_profile.session_snapshot`).
실행마다 확정 receipt/session manifest 집합을 한 번 고정하고 대상일과 재집계일에
그 객체를 전달한다. 따라서 실행 중 새 manifest가 도착해도 현재 창은 움직이지 않고,
새 입력은 다음 실행이 선택한다.

스냅샷 토큰은 세션 슬라이스가 아니라 **일별 사용 요약**의 ``config_version``에서
읽는다. 세션이 하나도 없는 날은 슬라이스 자체가 생기지 않아, 슬라이스만 보면
"세션이 없던 날"과 "아직 다시 집계하지 않은 날"을 구분할 수 없다. 사용 요약은
집계가 돌았다면 반드시 남으므로 그 구분이 된다.

이 모듈은 Spark도 DB도 직접 알지 않는다. 단계 구현은 :class:`SparkDailyStages`가
맡고, 순서와 재시도만 여기에 둔다. 덕분에 순서 자체를 Spark 없이 시험할 수 있다.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import logging
import time

from power_silver.analysis_job import (
    run_analysis_daily,
    select_analysis_input_snapshot,
)
from power_silver.catalog import SilverCatalog
from power_silver.commit import SilverCommitRepository
from power_silver.constants import (
    DATASET_APPLIANCE_USAGE_DAILY,
    DATASET_OBSERVATION,
    DATASET_POWER_CLEAN,
    DATASET_SESSION_SLICES,
    RUN_SKIPPED,
    RUN_SUCCEEDED,
    RUN_WAITING_INPUT,
)
from power_silver.job import run_daily
from power_silver.targets import load_targets

from gold_profile.input_snapshot import build_profile_snapshot, window_dates
from gold_profile.job import run_gold_profile
from gold_profile.session_snapshot import inspect_session_snapshot


logger = logging.getLogger(__name__)

STAGE_POWER_SILVER = "power-silver"
STAGE_USAGE_DAILY = "usage-daily"
STAGE_ALIGN_WINDOW = "align-window"
STAGE_GOLD_PROFILE = "gold-profile"
STAGE_PUBLISH = "publish"

STATUS_SUCCEEDED = "SUCCEEDED"
STATUS_FAILED = "FAILED"
STATUS_SKIPPED = "SKIPPED"
#: 단계는 끝났지만 남은 일이 있다. 다음 주기가 이어받는다.
STATUS_PARTIAL = "PARTIAL"

RUN_INPUT_INCOMPLETE = "INPUT_INCOMPLETE"
RUN_PUBLISH_PENDING = "PUBLISH_PENDING"

#: 창을 맞추지 못한 이유.
ALIGN_NO_TARGET_TOKEN = "NO_TARGET_TOKEN"
ALIGN_INPUT_MISSING = "INPUT_MISSING"
ALIGN_FINAL_MISMATCH = "FINAL_ALIGNMENT_FAILED"
# 이전 코드/대시보드가 import하는 이름은 유지한다.
ALIGN_SNAPSHOT_MOVING = ALIGN_FINAL_MISMATCH


class StageFailed(RuntimeError):
    """한 단계가 정해진 횟수만큼 다시 시도하고도 끝내 실패했다."""


class PipelineStopped(RuntimeError):
    def __init__(self, status: str):
        super().__init__(status)
        self.status = status


class InputNotReady(RuntimeError):
    """The upstream manifest boundary has not made the target date final yet."""


@dataclass
class StageReport:
    name: str
    status: str = STATUS_FAILED
    attempts: int = 0
    detail: dict = field(default_factory=dict)
    error: str | None = None

    def as_dict(self) -> dict:
        return {
            "stage": self.name,
            "status": self.status,
            "attempts": self.attempts,
            "detail": self.detail,
            "error": self.error,
        }


def run_daily_pipeline(
    stages,
    as_of_date: date,
    *,
    attempts: int = 3,
    retry_seconds: float = 30.0,
    align_window: bool = True,
    publish: bool = True,
    alignment_passes: int = 3,
    sleep=time.sleep,
) -> dict:
    """한 업무 날짜를 끝까지 흘린다.

    :param stages: 단계 구현. :class:`SparkDailyStages`를 참고한다
    :param attempts: 단계 하나를 다시 시도할 최대 횟수
    :param alignment_passes: 이전 호출자 호환용. 입력을 고정하므로 한 번의 재집계와
        최종 검증만 수행한다
    :return: 단계별 결과가 담긴 보고서. 예외를 밖으로 내지 않는다
    """

    reports: list[StageReport] = []

    def attempt(name: str, call, detail: dict | None = None):
        stage = StageReport(name, detail=dict(detail or {}))
        reports.append(stage)
        for number in range(1, attempts + 1):
            stage.attempts = number
            try:
                result = call()
            except Exception as error:
                stage.error = f"{type(error).__name__}: {error}"
                logger.warning(
                    "%s 실패 (%s/%s): %s", name, number, attempts, stage.error
                )
                if number >= attempts:
                    if isinstance(error, InputNotReady):
                        stage.status = STATUS_PARTIAL
                        stage.detail["status"] = RUN_WAITING_INPUT
                        raise PipelineStopped(RUN_INPUT_INCOMPLETE) from error
                    raise StageFailed(f"{name}: {stage.error}") from error
                sleep(retry_seconds)
                continue
            stage.status = STATUS_SUCCEEDED
            stage.error = None
            return stage, result
        raise AssertionError("unreachable")

    def power_silver():
        status = stages.power_silver(as_of_date)
        # SKIPPED는 다른 작성자가 그 날짜를 맡았다는 뜻이라 실패가 아니다.
        # WAITING_INPUT은 입력이 아직 안 왔다는 뜻이므로 다시 시도한다.
        if status == RUN_WAITING_INPUT:
            raise InputNotReady(f"status={status}")
        if status not in (RUN_SUCCEEDED, RUN_SKIPPED):
            raise RuntimeError(f"status={status}")
        return status

    outcome = STATUS_SUCCEEDED
    try:
        stage, status = attempt(STAGE_POWER_SILVER, power_silver)
        stage.detail["status"] = status
        if status == RUN_SKIPPED:
            stage.status = STATUS_SKIPPED
            raise PipelineStopped(STATUS_SKIPPED)

        selected = {"input": None}

        def usage_daily():
            if selected["input"] is None:
                selected["input"] = stages.select_analysis_input()
            return stages.usage_daily(as_of_date, selected["input"])

        stage, _manifest = attempt(
            STAGE_USAGE_DAILY,
            usage_daily,
        )
        selected_input = selected["input"]
        stage.detail.update({
            "input_snapshot_id": selected_input.snapshot_id,
            "receipt_snapshot_id": selected_input.receipt_snapshot_id,
            "session_snapshot_id": selected_input.session_snapshot_id,
        })

        if align_window:
            stage, detail = attempt(
                STAGE_ALIGN_WINDOW,
                lambda: _align_window(
                    stages, as_of_date, selected_input, alignment_passes
                ),
            )
            stage.detail.update(detail)
            if not detail["aligned"]:
                stage.status = STATUS_PARTIAL
                # An operational Gold profile cannot be proved safe from this
                # window.  A later run/backfill must repair it first.
                raise PipelineStopped(RUN_INPUT_INCOMPLETE)
        else:
            reports.append(StageReport(STAGE_ALIGN_WINDOW, STATUS_SKIPPED))

        def gold_profile():
            snapshot = stages.profile_input_snapshot(
                as_of_date, selected_input.session_token
            )
            result = stages.gold_profile(as_of_date, snapshot)
            if result.status not in (RUN_SUCCEEDED, RUN_SKIPPED):
                raise RuntimeError(f"status={result.status}")
            return snapshot, result

        stage, (gold_input, result) = attempt(STAGE_GOLD_PROFILE, gold_profile)
        stage.detail.update({
            "status": result.status,
            "incomplete": result.incomplete,
            "run_id": result.run_id,
            "reused_run_id": result.reused_run_id,
            "input_snapshot_id": gold_input.snapshot_id,
        })
        if result.status == RUN_SKIPPED:
            stage.status = STATUS_SKIPPED
            raise PipelineStopped(STATUS_SKIPPED)
        if result.incomplete:
            stage.status = STATUS_PARTIAL
            raise PipelineStopped(RUN_INPUT_INCOMPLETE)

        if publish:
            stage, counts = attempt(STAGE_PUBLISH, stages.publish)
            published, failed = counts
            stage.detail.update({
                "outbox_messages_published": published,
                "outbox_messages_failed": failed,
            })
            if failed:
                # 아웃박스 행은 남아 있다. 발행자가 다음 주기에 다시 보낸다.
                stage.status = STATUS_PARTIAL
                outcome = RUN_PUBLISH_PENDING
        else:
            reports.append(StageReport(STAGE_PUBLISH, STATUS_SKIPPED))
            outcome = RUN_PUBLISH_PENDING
    except PipelineStopped as stopped:
        outcome = stopped.status
    except StageFailed as error:
        outcome = STATUS_FAILED
        logger.error("일일 파이프라인이 멈췄다: %s", error)

    return {
        "as_of_date": as_of_date.isoformat(),
        "status": outcome,
        "ok": outcome == STATUS_SUCCEEDED,
        "stages": [item.as_dict() for item in reports],
    }


def _align_window(stages, as_of_date: date, selected_input, passes: int) -> dict:
    """창 안의 모든 날짜를 대상 날짜와 같은 세션 스냅샷으로 맞춘다.

    토큰이 없는 날짜(아직 집계하지 않은 날)도 어긋난 것으로 본다. 그대로 두면
    Gold가 창이 모자란 프로필을 만들고, 소비자는 그 프로필을 거절한다.
    """

    dates = stages.window_dates(as_of_date)
    expected_token = selected_input.session_token
    before = stages.session_alignment(dates, expected_token)
    if before.aligned:
        return _alignment(before, [], set(), 1, None)

    stale = set(before.missing_dates)
    stale.update(before.provenance_missing_dates)
    stale.update(before.version_mismatch_dates)
    stale.update(before.different_token_dates)
    ready = stages.aggregatable_dates(stale)
    skipped = stale - ready
    rebuilt: list[date] = []
    logger.info("고정 입력으로 세션 스냅샷을 맞추려고 %s일을 다시 집계한다", len(ready))
    for day in dates:
        if day in ready:
            stages.usage_daily(day, selected_input)
            rebuilt.append(day)

    # Do not infer success from the work list.  Re-read ACTIVE versions using
    # exactly the same predicate Gold applies, including skipped/missing dates.
    final = stages.session_alignment(dates, expected_token)
    if final.aligned:
        return _alignment(final, rebuilt, skipped, 1, None)
    reason = ALIGN_INPUT_MISSING if skipped or final.missing_dates else ALIGN_SNAPSHOT_MOVING
    return _alignment(final, rebuilt, skipped, min(1, passes), reason)


def _alignment(check, rebuilt, skipped, passes, reason) -> dict:
    def days(items):
        return sorted(day.isoformat() for day in items)

    return {
        "aligned": check.aligned,
        "reason": reason,
        "passes": passes,
        "rebuilt_dates": [day.isoformat() for day in rebuilt],
        "skipped_dates": days(skipped),
        "missing_dates": days(check.missing_dates),
        "missing_usage_dates": days(check.missing_usage_dates),
        "missing_slice_dates": days(check.missing_slice_dates),
        "provenance_missing_dates": days(check.provenance_missing_dates),
        "version_mismatch_dates": days(check.version_mismatch_dates),
        "different_token_dates": days(check.different_token_dates),
        "tokens_by_date": {
            day.isoformat(): token
            for day, token in sorted(check.tokens_by_date.items())
        },
    }


class SparkDailyStages:
    """실제 배치를 부르는 단계 구현.

    Spark 세션 하나를 모든 단계가 함께 쓴다. 단계마다 새로 띄우면 28일치 재집계에서
    세션 생성 비용이 계산보다 커진다.
    """

    def __init__(self, settings, *, storage, session_factory, spark, publisher):
        self._settings = settings
        self._storage = storage
        self._sessions = session_factory
        self._spark = spark
        self._publisher = publisher
        self._catalog = SilverCatalog(session_factory)
        self._repository = SilverCommitRepository(session_factory)
        self._targets = load_targets(settings.observation_targets_file)

    def window_dates(self, as_of_date: date) -> tuple[date, ...]:
        return window_dates(as_of_date, self._settings.profile_window_days)

    def power_silver(self, day: date) -> str:
        result = run_daily(
            self._settings, day,
            storage=self._storage, targets=self._targets,
            repository=self._repository, spark=self._spark, force=False,
        )
        logger.info(
            "power-silver %s: status=%s run_id=%s wait=%s",
            day, result.status, result.run_id, result.wait_reason,
        )
        return result.status

    def select_analysis_input(self):
        return select_analysis_input_snapshot(self._settings, storage=self._storage)

    def usage_daily(self, day: date, input_snapshot=None) -> dict:
        manifest = run_analysis_daily(
            self._settings, day,
            storage=self._storage, session_factory=self._sessions, spark=self._spark,
            input_snapshot=input_snapshot,
        )
        logger.info("analysis-usage-daily %s: run_id=%s", day, manifest["run_id"])
        return manifest

    def _analysis_versions(self, dates):
        return (
            self._catalog.active_versions(DATASET_APPLIANCE_USAGE_DAILY, dates),
            self._catalog.active_versions(DATASET_SESSION_SLICES, dates),
        )

    def session_alignment(self, dates, expected_token):
        usage, slices = self._analysis_versions(dates)
        return inspect_session_snapshot(
            usage.values(), slices.values(), dates, expected_token=expected_token
        )

    def aggregatable_dates(self, dates) -> set[date]:
        dates = tuple(dates)
        power = self._catalog.active_versions(DATASET_POWER_CLEAN, dates)
        observation = self._catalog.active_versions(DATASET_OBSERVATION, dates)
        return {day for day in dates if day in power and day in observation}

    def profile_input_snapshot(self, as_of_date: date, expected_token: str):
        dates = self.window_dates(as_of_date)
        usage, slices = self._analysis_versions(dates)
        check = inspect_session_snapshot(
            usage.values(), slices.values(), dates, expected_token=expected_token
        )
        if not check.aligned:
            raise RuntimeError("analysis versions changed before Gold input selection")
        return build_profile_snapshot(
            as_of_date,
            self._settings.profile_window_days,
            usage,
            slices,
            rule_version=self._settings.profile_rule_version,
            statistic_rule_version=self._settings.profile_statistic_rule_version,
            analysis_run_id=self._settings.analysis_run_id,
            timezone_name=f"UTC{self._settings.business_utc_offset_seconds:+d}s",
        )

    def gold_profile(self, as_of_date: date, input_snapshot=None):
        result = run_gold_profile(
            self._settings, as_of_date,
            storage=self._storage, session_factory=self._sessions,
            spark=self._spark, force=False, input_snapshot=input_snapshot,
        )
        logger.info(
            "gold-profile %s: status=%s run=%s reused=%s incomplete=%s",
            as_of_date, result.status, result.run_id,
            result.reused_run_id, result.incomplete,
        )
        # The orchestrator needs the run/reuse/incomplete fields as well as the
        # status to produce its result contract and decide whether to publish.
        return result

    def publish(self) -> tuple[int, int]:
        if self._publisher is None:
            raise RuntimeError("publisher is not configured")
        return self._publisher.publish_pending()

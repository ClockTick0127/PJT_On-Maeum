"""하루치 파이프라인의 순서, 창 정렬, 실패 복구."""

from datetime import date, timedelta
import threading
from types import SimpleNamespace
from uuid import uuid4

import gold_profile.daily as daily_module
from gold_profile.daily import (
    ALIGN_INPUT_MISSING,
    ALIGN_SNAPSHOT_MOVING,
    STAGE_ALIGN_WINDOW,
    STAGE_GOLD_PROFILE,
    STAGE_PUBLISH,
    STAGE_USAGE_DAILY,
    STATUS_PARTIAL,
    STATUS_SKIPPED,
    STATUS_SUCCEEDED,
    RUN_INPUT_INCOMPLETE,
    RUN_PUBLISH_PENDING,
    SparkDailyStages,
    run_daily_pipeline,
)
from gold_profile.delivery import drain_outbox
from gold_profile.session_snapshot import inspect_session_snapshot


AS_OF = date(2026, 9, 20)
WINDOW = tuple(AS_OF - timedelta(days=offset) for offset in (2, 1, 0))


def test_spark_daily_stage_preserves_gold_result_contract(monkeypatch):
    expected = SimpleNamespace(
        status="SUCCEEDED", run_id="run-1", reused_run_id=None, incomplete=False
    )
    monkeypatch.setattr(daily_module, "run_gold_profile", lambda *_args, **_kwargs: expected)
    stages = object.__new__(SparkDailyStages)
    stages._settings = SimpleNamespace()
    stages._storage = object()
    stages._sessions = object()
    stages._spark = object()

    assert stages.gold_profile(AS_OF) is expected


def test_spark_daily_stages_forward_the_fixed_input_to_the_real_batches(monkeypatch):
    """단계 구현이 고정 입력을 실제 배치 함수에 그대로 넘기는지 확인한다.

    FakeStages 테스트는 순서와 재시도만 본다. 여기서는 SparkDailyStages가
    ``run_analysis_daily``에 선택한 입력 스냅샷을, ``run_gold_profile``에 프로필 입력
    스냅샷을 넘기고, power-silver의 상태 문자열만 돌려주는지를 본다. 이 연결이 끊기면
    FakeStages 테스트는 모두 통과한 채 운영에서만 창이 움직인다.
    """

    calls = {}
    selected = SimpleNamespace(session_token="t1", snapshot_id="analysis-input")
    gold_input = SimpleNamespace(snapshot_id="gold-input")

    def fake_run_daily(settings, day, **kwargs):
        calls["power"] = (day, kwargs)
        return SimpleNamespace(status="WAITING_INPUT", run_id=None, wait_reason="PARTITIONS_BEHIND:3")

    def fake_run_analysis_daily(settings, day, **kwargs):
        calls["usage"] = (day, kwargs)
        return {"run_id": "analysis-run"}

    def fake_run_gold_profile(settings, day, **kwargs):
        calls["gold"] = (day, kwargs)
        return SimpleNamespace(status="SUCCEEDED", run_id="gold-run", reused_run_id=None, incomplete=False)

    monkeypatch.setattr(daily_module, "run_daily", fake_run_daily)
    monkeypatch.setattr(daily_module, "run_analysis_daily", fake_run_analysis_daily)
    monkeypatch.setattr(daily_module, "run_gold_profile", fake_run_gold_profile)
    monkeypatch.setattr(
        daily_module, "select_analysis_input_snapshot",
        lambda settings, *, storage: calls.setdefault("select", storage) and selected,
    )
    stages = object.__new__(SparkDailyStages)
    stages._settings = SimpleNamespace(profile_window_days=3)
    stages._storage = storage = object()
    stages._sessions = sessions = object()
    stages._spark = spark = object()
    stages._repository = repository = object()
    stages._targets = targets = object()

    assert stages.power_silver(AS_OF) == "WAITING_INPUT"
    assert calls["power"][0] == AS_OF
    assert calls["power"][1]["repository"] is repository
    assert calls["power"][1]["targets"] is targets
    assert calls["power"][1]["force"] is False

    assert stages.select_analysis_input() is selected
    assert calls["select"] is storage

    stages.usage_daily(AS_OF - timedelta(days=1), selected)
    day, kwargs = calls["usage"]
    assert day == AS_OF - timedelta(days=1)
    assert kwargs["input_snapshot"] is selected
    assert kwargs["storage"] is storage
    assert kwargs["session_factory"] is sessions
    assert kwargs["spark"] is spark

    result = stages.gold_profile(AS_OF, gold_input)
    assert result.run_id == "gold-run"
    assert calls["gold"][1]["input_snapshot"] is gold_input
    assert calls["gold"][1]["force"] is False


class FakeStages:
    """진짜 배치 대신 호출만 기록한다.

    세션 스냅샷을 실제 동작과 같게 흉내낸다. 집계가 한 번 돌면 그 날짜는 그 시점의
    전역 토큰을 갖는다. 전날만 집계하면 그 날짜만 새 토큰을 갖는 이유가 이것이다.
    """

    def __init__(
        self,
        *,
        tokens,
        current_token="t2",
        aggregatable=None,
        publish=(1, 0),
        fails=None,
        moving=False,
        missing_slices=None,
        missing_provenance=None,
        gold_result=None,
        power_status="SUCCEEDED",
        window=WINDOW,
    ):
        self.tokens = dict(tokens)
        self.window = tuple(window)
        self.current_token = current_token
        self.aggregatable = set(self.window if aggregatable is None else aggregatable)
        self.publish_result = publish
        self.fails = dict(fails or {})
        self.moving = moving
        self.missing_slices = set(missing_slices or ())
        self.missing_provenance = set(missing_provenance or ())
        self.gold_result = gold_result or SimpleNamespace(
            status="SUCCEEDED", incomplete=False, run_id="gold-run",
            reused_run_id=None,
        )
        self.power_status = power_status
        self.calls = []
        self.selected = None

    def _maybe_fail(self, name):
        left = self.fails.get(name, 0)
        if left:
            self.fails[name] = left - 1
            raise RuntimeError(f"{name} 실패")

    def window_dates(self, as_of_date):
        return self.window

    def power_silver(self, day):
        self.calls.append(("power-silver", day))
        self._maybe_fail("power-silver")
        return self.power_status

    def select_analysis_input(self):
        self.calls.append(("select-input", None))
        self.selected = SimpleNamespace(
            snapshot_id=f"input-{self.current_token}",
            receipt_snapshot_id="receipts-fixed",
            session_snapshot_id=self.current_token,
            session_token=self.current_token,
        )
        return self.selected

    def usage_daily(self, day, input_snapshot=None):
        self.calls.append(("usage-daily", day))
        self._maybe_fail("usage-daily")
        self.tokens[day] = input_snapshot.session_token
        self.missing_slices.discard(day)
        self.missing_provenance.discard(day)
        if self.moving:
            # 새 manifest는 도착하지만 이미 선택한 입력에는 영향을 주지 않는다.
            self.current_token += "+"
        return {"run_id": f"analysis-{day}"}

    def session_alignment(self, dates, expected_token):
        usage = []
        slices = []
        for day in dates:
            if day not in self.tokens:
                continue
            token = self.tokens[day]
            config = "legacy" if day in self.missing_provenance else f"sessions={token}"
            run_id = uuid4()
            usage.append(SimpleNamespace(
                target_date=day, run_id=run_id, config_version=config
            ))
            if day not in self.missing_slices:
                slices.append(SimpleNamespace(
                    target_date=day, run_id=run_id, config_version=config
                ))
        return inspect_session_snapshot(
            usage, slices, dates, expected_token=expected_token
        )

    def aggregatable_dates(self, dates):
        return {day for day in dates if day in self.aggregatable}

    def profile_input_snapshot(self, as_of_date, expected_token):
        check = self.session_alignment(self.window, expected_token)
        assert check.aligned
        return SimpleNamespace(snapshot_id="gold-input")

    def gold_profile(self, as_of_date, input_snapshot=None):
        self.calls.append(("gold-profile", as_of_date))
        self._maybe_fail("gold-profile")
        return self.gold_result

    def publish(self):
        self.calls.append(("publish", None))
        self._maybe_fail("publish")
        return self.publish_result


def run(stages, **kwargs):
    kwargs.setdefault("sleep", lambda _seconds: None)
    return run_daily_pipeline(stages, AS_OF, **kwargs)


def stage(report, name):
    return next(item for item in report["stages"] if item["stage"] == name)


def usage_days(stages):
    return [day for name, day in stages.calls if name == "usage-daily"]


def test_stages_run_in_order_and_a_matching_window_is_left_alone():
    stages = FakeStages(tokens={day: "t2" for day in WINDOW}, current_token="t2")

    report = run(stages)

    assert report["ok"] is True
    assert [name for name, _ in stages.calls] == [
        "power-silver", "select-input", "usage-daily", "gold-profile", "publish",
    ]
    # 이미 같은 스냅샷이다. 28일을 공연히 다시 집계하지 않는다.
    assert stage(report, STAGE_ALIGN_WINDOW)["detail"]["rebuilt_dates"] == []


def test_waiting_power_input_is_distinct_from_code_failure():
    stages = FakeStages(
        tokens={day: "t2" for day in WINDOW}, power_status="WAITING_INPUT"
    )

    report = run(stages, attempts=2)

    assert report["status"] == RUN_INPUT_INCOMPLETE
    assert report["ok"] is False
    assert report["stages"][0]["status"] == STATUS_PARTIAL
    assert report["stages"][0]["attempts"] == 2
    assert report["stages"][0]["detail"]["status"] == "WAITING_INPUT"


def test_window_dates_left_on_an_older_session_snapshot_are_rebuilt_before_gold():
    # 어제만 집계하면 어제만 새 토큰을 갖는다. Gold는 그 창을 거절한다.
    stages = FakeStages(tokens={day: "t1" for day in WINDOW}, current_token="t2")

    report = run(stages)

    assert report["ok"] is True
    assert usage_days(stages) == [AS_OF, WINDOW[0], WINDOW[1]]
    detail = stage(report, STAGE_ALIGN_WINDOW)["detail"]
    assert detail["aligned"] is True
    assert detail["rebuilt_dates"] == [WINDOW[0].isoformat(), WINDOW[1].isoformat()]
    # 정렬이 끝난 뒤에 Gold가 돈다.
    assert stages.calls.index(("gold-profile", AS_OF)) > stages.calls.index(
        ("usage-daily", WINDOW[1]))


def test_a_fixed_input_aligns_an_entire_28_day_window():
    days = tuple(AS_OF - timedelta(days=offset) for offset in range(27, -1, -1))
    stages = FakeStages(
        tokens={day: "before" for day in days}, current_token="selected", window=days
    )

    report = run(stages)

    detail = stage(report, STAGE_ALIGN_WINDOW)["detail"]
    assert report["status"] == STATUS_SUCCEEDED
    assert detail["aligned"] is True
    assert len(detail["tokens_by_date"]) == 28
    assert set(detail["tokens_by_date"].values()) == {"selected"}


def test_a_date_that_was_never_aggregated_counts_as_out_of_alignment():
    # 토큰이 없는 날짜를 그냥 두면 Gold가 창이 모자란 프로필을 만든다.
    stages = FakeStages(tokens={AS_OF: "t1"}, current_token="t2")

    report = run(stages)

    assert usage_days(stages) == [AS_OF, WINDOW[0], WINDOW[1]]
    assert stage(report, STAGE_ALIGN_WINDOW)["detail"]["aligned"] is True


def test_dates_without_power_silver_are_reported_instead_of_retried_forever():
    stages = FakeStages(
        tokens={day: "t1" for day in WINDOW},
        current_token="t2",
        aggregatable={AS_OF},
    )

    report = run(stages)

    # 운영 Gold는 불완전한 창에서 만들지 않는다.
    assert report["ok"] is False
    assert report["status"] == RUN_INPUT_INCOMPLETE
    detail = stage(report, STAGE_ALIGN_WINDOW)["detail"]
    assert detail["aligned"] is False
    assert detail["reason"] == ALIGN_INPUT_MISSING
    assert detail["skipped_dates"] == [WINDOW[0].isoformat(), WINDOW[1].isoformat()]
    assert stage(report, STAGE_ALIGN_WINDOW)["status"] == STATUS_PARTIAL
    assert ("gold-profile", AS_OF) not in stages.calls


def test_new_manifests_arriving_during_rebuild_do_not_move_the_selected_input():
    # 집계하는 사이 새 manifest가 계속 와도 모든 날짜는 고정 입력을 쓴다.
    stages = FakeStages(
        tokens={day: "t1" for day in WINDOW}, current_token="t2", moving=True
    )

    report = run(stages, alignment_passes=2)

    detail = stage(report, STAGE_ALIGN_WINDOW)["detail"]
    assert detail["aligned"] is True
    assert set(detail["tokens_by_date"].values()) == {"t2"}
    assert report["ok"] is True


def test_one_manifest_arriving_just_after_target_day_waits_for_next_run():
    class OneArrival(FakeStages):
        def usage_daily(self, day, input_snapshot=None):
            result = super().usage_daily(day, input_snapshot)
            self.moving = False
            return result

    stages = OneArrival(
        tokens={day: "t1" for day in WINDOW}, current_token="t2", moving=True
    )

    report = run(stages)

    detail = stage(report, STAGE_ALIGN_WINDOW)["detail"]
    assert report["ok"] is True
    assert set(detail["tokens_by_date"].values()) == {"t2"}
    assert stages.current_token == "t2+"


def test_a_missing_slice_version_is_not_reported_as_aligned():
    stages = FakeStages(
        tokens={day: "t2" for day in WINDOW}, current_token="t2",
        missing_slices={WINDOW[0]}, aggregatable={AS_OF},
    )

    report = run(stages)

    detail = stage(report, STAGE_ALIGN_WINDOW)["detail"]
    assert detail["aligned"] is False
    assert detail["missing_slice_dates"] == [WINDOW[0].isoformat()]
    assert report["status"] == RUN_INPUT_INCOMPLETE


def test_missing_session_provenance_is_not_reported_as_aligned():
    stages = FakeStages(
        tokens={day: "t2" for day in WINDOW}, current_token="t2",
        missing_provenance={WINDOW[0]}, aggregatable={AS_OF},
    )

    report = run(stages)

    detail = stage(report, STAGE_ALIGN_WINDOW)["detail"]
    assert detail["provenance_missing_dates"] == [WINDOW[0].isoformat()]
    assert detail["aligned"] is False


def test_a_stage_is_retried_and_a_dead_stage_stops_the_stages_after_it():
    stages = FakeStages(
        tokens={day: "t2" for day in WINDOW}, current_token="t2",
        fails={"gold-profile": 9},
    )

    report = run(stages, attempts=2)

    assert report["ok"] is False
    assert stage(report, STAGE_GOLD_PROFILE)["attempts"] == 2
    assert "gold-profile 실패" in stage(report, STAGE_GOLD_PROFILE)["error"]
    # 프로필이 없는데 발행할 것은 없다.
    assert not any(name == "publish" for name, _ in stages.calls)


def test_a_transient_failure_is_absorbed_by_the_retry():
    stages = FakeStages(
        tokens={day: "t2" for day in WINDOW}, current_token="t2",
        fails={"usage-daily": 1},
    )

    report = run(stages, attempts=3)

    assert report["ok"] is True
    assert stage(report, STAGE_USAGE_DAILY)["attempts"] == 2
    assert stage(report, STAGE_USAGE_DAILY)["status"] == STATUS_SUCCEEDED


def test_an_undelivered_profile_does_not_fail_the_day():
    # 아웃박스 행은 남아 있고 상주 발행자가 다시 보낸다. 하루를 실패로 볼 일이 아니다.
    stages = FakeStages(
        tokens={day: "t2" for day in WINDOW}, current_token="t2", publish=(0, 2)
    )

    report = run(stages)

    assert report["ok"] is False
    assert stage(report, STAGE_PUBLISH)["status"] == STATUS_PARTIAL
    assert report["status"] == RUN_PUBLISH_PENDING
    assert stage(report, STAGE_PUBLISH)["detail"] == {
        "outbox_messages_published": 0,
        "outbox_messages_failed": 2,
    }


def test_an_input_incomplete_gold_result_is_preserved_and_not_published():
    result = SimpleNamespace(
        status="SUCCEEDED", incomplete=True, run_id="new-run", reused_run_id=None
    )
    stages = FakeStages(
        tokens={day: "t2" for day in WINDOW}, current_token="t2",
        gold_result=result,
    )

    report = run(stages)

    gold = stage(report, STAGE_GOLD_PROFILE)
    assert report["status"] == RUN_INPUT_INCOMPLETE
    assert report["ok"] is False
    assert gold["detail"]["incomplete"] is True
    assert gold["detail"]["run_id"] == "new-run"
    assert ("publish", None) not in stages.calls


def test_a_skipped_gold_lock_is_not_reported_as_generated():
    result = SimpleNamespace(
        status="SKIPPED", incomplete=False, run_id=None, reused_run_id=None
    )
    stages = FakeStages(
        tokens={day: "t2" for day in WINDOW}, current_token="t2",
        gold_result=result,
    )

    report = run(stages)

    assert report["status"] == STATUS_SKIPPED
    assert report["ok"] is False
    assert stage(report, STAGE_GOLD_PROFILE)["status"] == STATUS_SKIPPED
    assert ("publish", None) not in stages.calls


class FakePublisher:
    def __init__(self, stop, *, sweeps, raises=False):
        self.stop = stop
        self.sweeps = sweeps
        self.raises = raises
        self.calls = 0

    def publish_pending(self):
        self.calls += 1
        if self.calls >= self.sweeps:
            self.stop.set()
        if self.raises:
            raise RuntimeError("데이터베이스가 끊겼다")
        return 1, 0


def test_the_publisher_loop_keeps_draining_until_it_is_told_to_stop():
    stop = threading.Event()
    publisher = FakePublisher(stop, sweeps=3)

    assert drain_outbox(publisher, 0, stop) == 0
    assert publisher.calls == 3


def test_the_publisher_loop_survives_a_failed_sweep():
    # 쓸어 담기가 실패한 것과 프로세스가 끝나야 하는 것은 다른 일이다.
    stop = threading.Event()
    publisher = FakePublisher(stop, sweeps=2, raises=True)

    assert drain_outbox(publisher, 0, stop) == 0
    assert publisher.calls == 2

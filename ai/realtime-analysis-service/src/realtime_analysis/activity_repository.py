"""가전 사용 세션과 일일 활동을 트랜잭션으로 저장한다."""

from collections.abc import Sequence
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import AbstractSet, Protocol
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from realtime_analysis.models import (
    AnalysisProcessingReceipt,
    AnalysisReceiptLakeOutbox,
    ApplianceUsageSession,
    HouseholdActivityDaily,
    HouseholdObservationDaily,
)
from realtime_analysis.processing_receipt import ProcessingReceipt
from realtime_analysis.schemas import (
    ApplianceState,
    ApplianceStateTransition,
    ApplianceTransitionType,
)

# SQLAlchemy 기반 저장 Repository

EXPECTED_SAMPLES_PER_DAY = 86_400
PROBABILITY_QUANTUM = Decimal("0.0001")
COVERAGE_QUANTUM = Decimal("0.0001")


class ApplianceActivityRepository(Protocol):
    """Handler와 DB 구현을 분리하기 위한 저장소 인터페이스."""

    def record_observation(
        self,
        household_id: str,
        observed_at: datetime,
    ) -> None: ...

    def record(
        self,
        household_id: str,
        observed_at: datetime,
        states: Sequence[ApplianceState],
        transitions: Sequence[ApplianceStateTransition],
        active_appliance_types: AbstractSet[str],
        processing_receipt: ProcessingReceipt | None = None,
    ) -> None: ...

    def record_processing_outcome(self, receipt: ProcessingReceipt) -> None: ...

    def close_open_sessions(
        self,
        household_id: str,
        ended_at: datetime,
    ) -> None: ...


class SqlAlchemyApplianceActivityRepository:
    """상태 변화와 세션 확률을 SQLAlchemy로 원자적으로 저장한다."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        timezone_name: str,
        expected_samples_per_day: int = EXPECTED_SAMPLES_PER_DAY,
    ) -> None:
        if expected_samples_per_day < 1:
            raise ValueError("expected_samples_per_day must be greater than zero")
        self._session_factory = session_factory
        self._timezone = ZoneInfo(timezone_name)
        self._expected_samples_per_day = expected_samples_per_day

    def record_observation(
        self,
        household_id: str,
        observed_at: datetime,
    ) -> None:
        """정상 입력 한 건을 일일 샘플 수와 수집률에 반영한다."""

        observation_date = observed_at.astimezone(self._timezone).date()
        with self._session_factory.begin() as session:
            observation = session.scalar(
                select(HouseholdObservationDaily)
                .where(
                    HouseholdObservationDaily.household_id == household_id,
                    HouseholdObservationDaily.observation_date == observation_date,
                )
                .with_for_update()
            )
            if observation is None:
                observation = HouseholdObservationDaily(
                    household_id=household_id,
                    observation_date=observation_date,
                    sample_count=1,
                    expected_sample_count=self._expected_samples_per_day,
                    coverage_ratio=self._coverage_ratio(1),
                    observation_status="COLLECTING",
                    updated_at=observed_at,
                )
                session.add(observation)
                return

            # 00:10 일일 마감 이후 도착한 지연 샘플은 이미 발행한 지수를 바꾸지 않는다.
            if observation.observation_status != "COLLECTING":
                return

            # Consumer는 가구별 메시지를 순서대로 처리한다. Snapshot 발행 후 Offset 커밋
            # 전에 재시작되어 같은 입력이 재처리되면 updated_at을 기준으로 중복 집계하지 않는다.
            if not self._is_newer_than_last_update(observed_at, observation.updated_at):
                return

            observation.sample_count += 1
            observation.coverage_ratio = self._coverage_ratio(
                observation.sample_count,
                observation.expected_sample_count,
            )
            observation.updated_at = observed_at

    def record(
        self,
        household_id: str,
        observed_at: datetime,
        states: Sequence[ApplianceState],
        transitions: Sequence[ApplianceStateTransition],
        active_appliance_types: AbstractSet[str],
        processing_receipt: ProcessingReceipt | None = None,
    ) -> None:
        if not transitions and not active_appliance_types and processing_receipt is None:
            return

        state_by_appliance = {state.appliance_type: state for state in states}

        # 이 블록 안의 INSERT/UPDATE는 모두 같은 DB 트랜잭션에 포함된다.
        # 정상 종료하면 한 번에 COMMIT하고, 하나라도 실패하면 전부 ROLLBACK한다.
        with self._session_factory.begin() as session:
            if processing_receipt is not None and self._success_exists(session, processing_receipt):
                return
            # OFF → ON: 일일 관측/활동 행을 준비하고 새 사용 세션을 생성한다.
            for transition in transitions:
                if transition.transition_type == ApplianceTransitionType.TURNED_ON:
                    self._start_session(session, transition)

            # 확정 상태가 ON인 동안 열린 세션의 최대 모델 확률을 갱신한다.
            # 히스테리시스 구간이어도 확정 상태가 ON이면 이 처리에 포함된다.
            for appliance_type in active_appliance_types:
                state = state_by_appliance.get(appliance_type)
                if state is not None:
                    self._update_max_probability(
                        session,
                        household_id,
                        appliance_type,
                        state.probability,
                        observed_at,
                    )

            # ON → OFF: 해당 가구·가전의 열린 세션에 종료 시각을 기록한다.
            for transition in transitions:
                if transition.transition_type == ApplianceTransitionType.TURNED_OFF:
                    self._finish_session(session, transition)

            if processing_receipt is not None:
                session.flush()
                refs = self._changed_session_refs(
                    session, household_id, observed_at
                )
                self._add_receipt(session, processing_receipt, refs)

    def record_processing_outcome(self, receipt: ProcessingReceipt) -> None:
        """Persist a non-success outcome and its lake outbox idempotently."""

        with self._session_factory.begin() as session:
            if receipt.outcome.value == "SUCCEEDED" and self._success_exists(session, receipt):
                return
            self._add_receipt(session, receipt, [])

    @staticmethod
    def _success_exists(session: Session, receipt: ProcessingReceipt) -> bool:
        return session.scalar(
            select(AnalysisProcessingReceipt.receipt_id).where(
                AnalysisProcessingReceipt.message_id == receipt.message_id,
                AnalysisProcessingReceipt.analysis_run_id == receipt.analysis_run_id,
                AnalysisProcessingReceipt.outcome == "SUCCEEDED",
            )
        ) is not None

    @staticmethod
    def _changed_session_refs(
        session: Session,
        household_id: str,
        observed_at: datetime,
    ) -> list[dict[str, object]]:
        changed = session.scalars(
            select(ApplianceUsageSession)
            .join(
                HouseholdActivityDaily,
                HouseholdActivityDaily.id == ApplianceUsageSession.activity_daily_id,
            )
            .join(
                HouseholdObservationDaily,
                HouseholdObservationDaily.id == HouseholdActivityDaily.observation_daily_id,
            )
            .where(
                HouseholdObservationDaily.household_id == household_id,
                ApplianceUsageSession.updated_at == observed_at,
            )
        ).all()
        return [
            {"session_id": str(item.id), "session_version": int(item.lake_version)}
            for item in changed
        ]

    @staticmethod
    def _add_receipt(
        session: Session,
        receipt: ProcessingReceipt,
        session_change_refs: list[dict[str, object]],
    ) -> None:
        appliance_types = sorted(set(receipt.appliance_types))
        attempt = int(
            session.scalar(
                select(func.coalesce(func.max(AnalysisProcessingReceipt.attempt), 0)).where(
                    AnalysisProcessingReceipt.message_id == receipt.message_id,
                    AnalysisProcessingReceipt.analysis_run_id == receipt.analysis_run_id,
                )
            )
            or 0
        ) + 1
        row = AnalysisProcessingReceipt(
            receipt_id=receipt.receipt_id,
            message_id=receipt.message_id,
            household_id=receipt.household_id,
            device_id=receipt.device_id,
            source_topic=receipt.source.topic,
            source_partition=receipt.source.partition,
            source_offset=receipt.source.offset,
            measured_at=receipt.measured_at,
            processed_at=receipt.processed_at,
            analysis_run_id=receipt.analysis_run_id,
            attempt=attempt,
            model_version=receipt.model_version,
            pipeline_version=receipt.pipeline_version,
            state_epoch=receipt.state_epoch,
            outcome=receipt.outcome.value,
            appliance_types=appliance_types,
            session_change_refs=session_change_refs,
            error_type=receipt.error_type,
        )
        payload = {
            "receipt_id": str(receipt.receipt_id),
            "message_id": str(receipt.message_id),
            "household_id": receipt.household_id,
            "device_id": receipt.device_id,
            "source_topic": receipt.source.topic,
            "source_partition": receipt.source.partition,
            "source_offset": receipt.source.offset,
            "measured_at": receipt.measured_at.isoformat(),
            "processed_at": receipt.processed_at.isoformat(),
            "analysis_run_id": receipt.analysis_run_id,
            "attempt": attempt,
            "model_version": receipt.model_version,
            "pipeline_version": receipt.pipeline_version,
            "state_epoch": str(receipt.state_epoch),
            "outcome": receipt.outcome.value,
            "appliance_types": appliance_types,
            "session_change_refs": session_change_refs,
            "error_type": receipt.error_type,
        }
        session.add(row)
        session.add(
            AnalysisReceiptLakeOutbox(
                receipt_id=receipt.receipt_id,
                payload=payload,
                changed_at=receipt.processed_at,
                delivery_status="PENDING",
            )
        )

    def close_open_sessions(
        self,
        household_id: str,
        ended_at: datetime,
    ) -> None:
        """Close sessions whose continuity can no longer be proven after a gap."""

        with self._session_factory.begin() as session:
            open_sessions = session.scalars(
                select(ApplianceUsageSession)
                .join(
                    HouseholdActivityDaily,
                    HouseholdActivityDaily.id
                    == ApplianceUsageSession.activity_daily_id,
                )
                .join(
                    HouseholdObservationDaily,
                    HouseholdObservationDaily.id
                    == HouseholdActivityDaily.observation_daily_id,
                )
                .where(
                    HouseholdObservationDaily.household_id == household_id,
                    ApplianceUsageSession.ended_at.is_(None),
                )
                .with_for_update()
            ).all()
            for usage_session in open_sessions:
                session_start = usage_session.started_at
                if session_start.tzinfo is None:
                    session_start = session_start.replace(tzinfo=self._timezone)
                safe_end = ended_at
                if (
                    session_start.astimezone(timezone.utc)
                    > ended_at.astimezone(timezone.utc)
                ):
                    safe_end = usage_session.started_at
                usage_session.ended_at = safe_end
                usage_session.updated_at = safe_end

    # OFF -> ON
    def _start_session(
        self,
        session: Session,
        transition: ApplianceStateTransition,
    ) -> None:
        probability = _as_probability(transition.probability)

        # 서비스 재처리 등으로 이미 열린 세션이 있으면 새 세션을 만들지 않는다.
        existing_open = self._find_open_session(
            session,
            transition.household_id,
            transition.appliance_type,
        )
        if existing_open is not None:
            self._set_max_probability(
                existing_open,
                probability,
                transition.confirmed_at,
            )
            return

        # 세션이 시작된 시각을 서비스 기준 시간대로 변환해 일일 집계 날짜를 정한다.
        activity_date = transition.occurred_at.astimezone(self._timezone).date()
        observation = self._get_or_create_observation(
            session,
            transition.household_id,
            activity_date,
            transition.confirmed_at,
        )
        if observation.observation_status != "COLLECTING":
            return
        activity = self._get_or_create_activity(
            session,
            observation,
            transition.appliance_type,
            transition.confirmed_at,
        )

        # 동일한 세션 시작 이벤트가 Kafka에서 다시 전달돼도 중복 INSERT하지 않는다.
        existing = session.scalar(
            select(ApplianceUsageSession).where(
                ApplianceUsageSession.activity_daily_id == activity.id,
                ApplianceUsageSession.started_at == transition.occurred_at,
            )
        )
        if existing is not None:
            self._set_max_probability(
                existing,
                probability,
                transition.confirmed_at,
            )
            return

        usage_session = ApplianceUsageSession(
            activity_daily_id=activity.id,
            started_at=transition.occurred_at,
            ended_at=None,
            max_probability=probability,
            decision_threshold=_as_probability(transition.threshold),
            updated_at=transition.confirmed_at,
        )
        session.add(usage_session)
        session.flush()

        # 세션 INSERT가 성공한 경우에만 사용 횟수를 증가시킨다.
        # 아래 변경도 같은 트랜잭션이므로 이후 오류가 나면 세션과 함께 롤백된다.
        activity.event_count += 1
        activity.updated_at = transition.confirmed_at

    # ON 유지
    def _update_max_probability(
        self,
        session: Session,
        household_id: str,
        appliance_type: str,
        probability: float,
        observed_at: datetime,
    ) -> None:
        usage_session = self._find_open_session(
            session,
            household_id,
            appliance_type,
        )
        # 열린 세션이 없으면 갱신할 대상이 없으므로 아무 작업도 하지 않는다.
        if usage_session is None:
            return
        self._set_max_probability(
            usage_session,
            _as_probability(probability),
            observed_at,
        )

    # ON -> OFF
    def _finish_session(
        self,
        session: Session,
        transition: ApplianceStateTransition,
    ) -> None:
        usage_session = self._find_open_session(
            session,
            transition.household_id,
            transition.appliance_type,
        )
        if usage_session is None:
            return

        # 연속 OFF 판정이 시작된 시각을 실제 사용 종료 시각으로 저장한다.
        usage_session.ended_at = transition.occurred_at
        usage_session.updated_at = transition.confirmed_at

    def _get_or_create_observation(
        self,
        session: Session,
        household_id: str,
        activity_date: date,
        updated_at: datetime,
    ) -> HouseholdObservationDaily:
        # 가구와 날짜의 UNIQUE 조건을 기준으로 기존 일일 관측 행을 조회한다.
        observation = session.scalar(
            select(HouseholdObservationDaily).where(
                HouseholdObservationDaily.household_id == household_id,
                HouseholdObservationDaily.observation_date == activity_date,
            )
        )
        if observation is not None:
            return observation

        # 정상 Handler 흐름에서는 record_observation이 먼저 부모 행을 만든다.
        # 직접 세션 저장을 호출하는 복구 경로를 위해 여기서도 부모 행 생성을 보장한다.
        observation = HouseholdObservationDaily(
            household_id=household_id,
            observation_date=activity_date,
            sample_count=0,
            expected_sample_count=self._expected_samples_per_day,
            coverage_ratio=Decimal("0.0000"),
            observation_status="COLLECTING",
            updated_at=updated_at,
        )
        session.add(observation)
        session.flush()
        return observation

    def _coverage_ratio(
        self,
        sample_count: int,
        expected_sample_count: int | None = None,
    ) -> Decimal:
        expected = expected_sample_count or self._expected_samples_per_day
        ratio = Decimal(sample_count) / Decimal(expected)
        return min(Decimal("1.0000"), ratio.quantize(COVERAGE_QUANTUM))

    def _is_newer_than_last_update(
        self,
        observed_at: datetime,
        updated_at: datetime,
    ) -> bool:
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=self._timezone)
        return observed_at.astimezone(timezone.utc) > updated_at.astimezone(timezone.utc)

    @staticmethod
    def _get_or_create_activity(
        session: Session,
        observation: HouseholdObservationDaily,
        appliance_type: str,
        updated_at: datetime,
    ) -> HouseholdActivityDaily:
        # 같은 일일 관측 결과와 가전 조합은 하나의 활동 행만 사용한다.
        activity = session.scalar(
            select(HouseholdActivityDaily).where(
                HouseholdActivityDaily.observation_daily_id == observation.id,
                HouseholdActivityDaily.appliance_type == appliance_type,
            )
        )
        if activity is not None:
            return activity

        activity = HouseholdActivityDaily(
            observation_daily_id=observation.id,
            appliance_type=appliance_type,
            event_count=0,
            updated_at=updated_at,
        )
        session.add(activity)
        session.flush()
        return activity

    @staticmethod
    def _find_open_session(
        session: Session,
        household_id: str,
        appliance_type: str,
    ) -> ApplianceUsageSession | None:
        # 부모 테이블을 JOIN해 가구·가전 기준의 종료되지 않은 세션을 찾는다.
        # FOR UPDATE로 조회해 현재 트랜잭션이 끝날 때까지 갱신 대상을 잠근다.
        return session.scalar(
            select(ApplianceUsageSession)
            .join(
                HouseholdActivityDaily,
                HouseholdActivityDaily.id
                == ApplianceUsageSession.activity_daily_id,
            )
            .join(
                HouseholdObservationDaily,
                HouseholdObservationDaily.id
                == HouseholdActivityDaily.observation_daily_id,
            )
            .where(
                HouseholdObservationDaily.household_id == household_id,
                HouseholdActivityDaily.appliance_type == appliance_type,
                ApplianceUsageSession.ended_at.is_(None),
            )
            .order_by(ApplianceUsageSession.started_at.desc())
            .with_for_update()
        )

    @staticmethod
    def _set_max_probability(
        usage_session: ApplianceUsageSession,
        probability: Decimal,
        updated_at: datetime,
    ) -> None:
        # max_probability이므로 기존 값보다 큰 확률만 저장한다.
        if probability > usage_session.max_probability:
            usage_session.max_probability = probability
            usage_session.updated_at = updated_at


def _as_probability(value: float) -> Decimal:
    # DB의 NUMERIC(5, 4)에 맞춰 부동소수점 값을 소수점 넷째 자리로 변환한다.
    return Decimal(str(value)).quantize(PROBABILITY_QUANTUM)

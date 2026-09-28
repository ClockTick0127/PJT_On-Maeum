"""SQLAlchemy mappings for the analysis_db MVP schema."""

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    JSON,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from realtime_analysis.database import Base


APPLIANCE_TYPES = (
    "KETTLE",
    "INDUCTION",
    "IRON",
    "MICROWAVE",
    "HAIR_DRYER",
    "VACUUM_CLEANER",
)
APPLIANCE_TYPE_SQL = ", ".join(f"'{item}'" for item in APPLIANCE_TYPES)


class ModelArtifact(Base):
    __tablename__ = "model_artifact"
    __table_args__ = (
        UniqueConstraint("model_name", "version", name="uq_model_artifact_name_version"),
        CheckConstraint(
            "status IN ('VALIDATED', 'ACTIVE', 'RETIRED')",
            name="status",
        ),
        Index(
            "uq_model_artifact_single_active",
            "status",
            unique=True,
            postgresql_where=text("status = 'ACTIVE'"),
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    model_name: Mapped[str] = mapped_column(String(100), nullable=False)
    version: Mapped[str] = mapped_column(String(50), nullable=False)
    artifact_uri: Mapped[str] = mapped_column(String(500), nullable=False)
    manifest_uri: Mapped[str] = mapped_column(String(500), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class RoutineBaselineModel(Base):
    __tablename__ = "routine_baseline"
    __table_args__ = (
        UniqueConstraint(
            "household_id",
            "appliance_type",
            "baseline_type",
            name="uq_routine_baseline_household_appliance_type",
        ),
        CheckConstraint(
            f"appliance_type IN ({APPLIANCE_TYPE_SQL})",
            name="appliance_type",
        ),
        CheckConstraint("sample_days >= 0", name="sample_days_nonnegative"),
        CheckConstraint("active_days >= 0", name="active_days_nonnegative"),
        CheckConstraint("active_days <= sample_days", name="active_days_lte_sample_days"),
        CheckConstraint(
            "daily_use_probability BETWEEN 0 AND 1",
            name="daily_use_probability",
        ),
        CheckConstraint(
            "reliability_weight BETWEEN 0 AND 1",
            name="reliability_weight",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    household_id: Mapped[str] = mapped_column(String(50), nullable=False)
    appliance_type: Mapped[str] = mapped_column(String(50), nullable=False)
    baseline_type: Mapped[str] = mapped_column(String(30), nullable=False)
    sample_days: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    active_days: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    daily_use_probability: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False)
    reliability_weight: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False)
    baseline_data: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"),
        nullable=False,
    )
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    calculated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AnalysisPolicy(Base):
    __tablename__ = "analysis_policy"
    __table_args__ = (
        UniqueConstraint("policy_code", name="uq_analysis_policy_code"),
        CheckConstraint("cooldown_hours >= 0", name="cooldown_hours_nonnegative"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    policy_code: Mapped[str] = mapped_column(String(50), nullable=False)
    algorithm_type: Mapped[str] = mapped_column(String(50), nullable=False)
    parameters: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    cooldown_hours: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class AnalysisEventEmission(Base):
    """Successfully published analysis events used for cooldown checks."""

    __tablename__ = "analysis_event_emission"
    __table_args__ = (
        Index(
            "ix_analysis_event_emission_cooldown",
            "household_id",
            "event_type",
            "appliance_type",
            "emitted_at",
        ),
    )

    event_id: Mapped[UUID] = mapped_column(primary_key=True)
    household_id: Mapped[str] = mapped_column(String(50), nullable=False)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    appliance_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    emitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class HouseholdOutingState(Base):
    """Latest outing state and latest interval received from monitoring."""

    __tablename__ = "household_outing_state"
    __table_args__ = (
        CheckConstraint(
            "NOT is_outing OR outing_started_at IS NOT NULL",
            name="outing_started_at_available_when_outing",
        ),
    )

    household_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    is_outing: Mapped[bool] = mapped_column(Boolean, nullable=False)
    outing_started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_returned_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_event_id: Mapped[UUID] = mapped_column(nullable=False)
    last_event_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class HouseholdObservationDaily(Base):
    __tablename__ = "household_observation_daily"
    __table_args__ = (
        UniqueConstraint(
            "household_id",
            "observation_date",
            name="uq_household_observation_daily_household_date",
        ),
        CheckConstraint("sample_count >= 0", name="sample_count_nonnegative"),
        CheckConstraint(
            "expected_sample_count >= 0",
            name="expected_sample_count_nonnegative",
        ),
        CheckConstraint("coverage_ratio BETWEEN 0 AND 1", name="coverage_ratio"),
        CheckConstraint(
            "observation_status IN "
            "('COLLECTING', 'VALID', 'INSUFFICIENT_DATA', 'SENSOR_GAP', "
            "'PROCESSING_ERROR', 'EXCLUDED')",
            name="observation_status",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    household_id: Mapped[str] = mapped_column(String(50), nullable=False)
    observation_date: Mapped[date] = mapped_column(Date, nullable=False)
    sample_count: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    expected_sample_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    coverage_ratio: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False)
    observation_status: Mapped[str] = mapped_column(String(30), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class HouseholdActivityDaily(Base):
    __tablename__ = "household_activity_daily"
    __table_args__ = (
        UniqueConstraint(
            "observation_daily_id",
            "appliance_type",
            name="uq_household_activity_daily_observation_appliance",
        ),
        CheckConstraint(
            f"appliance_type IN ({APPLIANCE_TYPE_SQL})",
            name="appliance_type",
        ),
        CheckConstraint("event_count >= 0", name="event_count_nonnegative"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    observation_daily_id: Mapped[UUID] = mapped_column(
        ForeignKey("household_observation_daily.id", ondelete="CASCADE"),
        nullable=False,
    )
    appliance_type: Mapped[str] = mapped_column(String(50), nullable=False)
    event_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class ApplianceUsageSession(Base):
    __tablename__ = "appliance_usage_session"
    __table_args__ = (
        UniqueConstraint(
            "activity_daily_id",
            "started_at",
            name="uq_appliance_usage_session_activity_started",
        ),
        CheckConstraint(
            "ended_at IS NULL OR ended_at >= started_at",
            name="valid_time_range",
        ),
        CheckConstraint("max_probability BETWEEN 0 AND 1", name="max_probability"),
        CheckConstraint("decision_threshold BETWEEN 0 AND 1", name="decision_threshold"),
        CheckConstraint("lake_version >= 1", name="lake_version_positive"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    activity_daily_id: Mapped[UUID] = mapped_column(
        ForeignKey("household_activity_daily.id", ondelete="CASCADE"),
        nullable=False,
    )
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    max_probability: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False)
    decision_threshold: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    # 레이크 전달용 내용 버전. PostgreSQL 트리거(마이그레이션 20260920_10)가
    # INSERT 시 1로 고정하고 내용이 실제로 바뀐 UPDATE마다 1씩 올린다.
    # 애플리케이션 코드는 이 값을 직접 쓰지 않는다.
    lake_version: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("1")
    )


ANALYSIS_RECEIPT_OUTCOMES = (
    "SUCCEEDED",
    "SKIPPED_WARMUP",
    "SKIPPED_QUALITY_GATE",
    "FAILED_INFERENCE",
    "FAILED_PERSISTENCE",
)


class AnalysisProcessingReceipt(Base):
    """Durable, idempotent evidence for one validated input and analysis run."""

    __tablename__ = "analysis_processing_receipt"
    __table_args__ = (
        UniqueConstraint(
            "message_id", "analysis_run_id", "attempt",
            name="uq_analysis_receipt_message_run_attempt"
        ),
        CheckConstraint(
            "outcome IN ('SUCCEEDED', 'SKIPPED_WARMUP', "
            "'SKIPPED_QUALITY_GATE', 'FAILED_INFERENCE', 'FAILED_PERSISTENCE')",
            name="outcome",
        ),
        CheckConstraint(
            "(source_partition IS NULL) = (source_offset IS NULL)",
            name="source_position_complete",
        ),
        Index("ix_analysis_receipt_household_measured", "household_id", "measured_at"),
    )

    receipt_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    message_id: Mapped[UUID] = mapped_column(nullable=False)
    household_id: Mapped[str] = mapped_column(String(50), nullable=False)
    device_id: Mapped[str] = mapped_column(String(50), nullable=False)
    source_topic: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_partition: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_offset: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    measured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    analysis_run_id: Mapped[str] = mapped_column(String(100), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    model_version: Mapped[str] = mapped_column(String(50), nullable=False)
    pipeline_version: Mapped[str] = mapped_column(String(50), nullable=False)
    state_epoch: Mapped[UUID] = mapped_column(nullable=False)
    outcome: Mapped[str] = mapped_column(String(40), nullable=False)
    appliance_types: Mapped[list[str]] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"), nullable=False, default=list
    )
    session_change_refs: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"), nullable=False, default=list
    )
    error_type: Mapped[str | None] = mapped_column(String(200), nullable=True)


class AnalysisReceiptLakeOutbox(Base):
    """Receipt payload captured atomically for Bronze delivery."""

    __tablename__ = "analysis_receipt_lake_outbox"
    __table_args__ = (
        CheckConstraint(
            "delivery_status IN ('PENDING', 'ASSIGNED', 'DELIVERED')",
            name="delivery_status",
        ),
        CheckConstraint(
            "(delivery_status = 'PENDING') = (batch_id IS NULL)",
            name="batch_assigned_matches_status",
        ),
        Index("ix_analysis_receipt_outbox_delivery", "delivery_status", "event_id"),
    )

    event_id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), Identity(), primary_key=True
    )
    receipt_id: Mapped[UUID] = mapped_column(
        ForeignKey("analysis_processing_receipt.receipt_id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"), nullable=False
    )
    changed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    delivery_status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=text("'PENDING'")
    )
    batch_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("analysis_receipt_lake_batch.batch_id"), nullable=True
    )
    delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class AnalysisReceiptLakeBatch(Base):
    __tablename__ = "analysis_receipt_lake_batch"
    __table_args__ = (
        CheckConstraint("status IN ('ASSIGNED', 'FAILED', 'COMPLETED')", name="status"),
        CheckConstraint("event_count >= 0", name="event_count_nonnegative"),
    )

    batch_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    ingest_date: Mapped[date] = mapped_column(Date, nullable=False)
    event_count: Mapped[int] = mapped_column(Integer, nullable=False)
    manifest_path: Mapped[str] = mapped_column(String(500), nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class AnalysisDailyCompletion(Base):
    """Revisioned declaration for a fixed daily input and analysis evidence set."""

    __tablename__ = "analysis_daily_completion"
    __table_args__ = (
        UniqueConstraint(
            "household_id", "appliance_type", "target_date", "revision",
            name="uq_analysis_daily_completion_scope_revision",
        ),
        CheckConstraint("revision >= 1", name="revision_positive"),
        CheckConstraint("coverage_ratio BETWEEN 0 AND 1", name="coverage_ratio"),
        CheckConstraint("max_unanalyzed_seconds >= 0", name="max_unanalyzed_seconds_nonnegative"),
        CheckConstraint("analysis_status IN ('COMPLETE', 'INCOMPLETE', 'ERROR', 'UNKNOWN')", name="analysis_status"),
        CheckConstraint("delivery_status IN ('COMPLETE', 'PENDING')", name="delivery_status"),
        Index("ix_analysis_daily_completion_scope", "target_date", "household_id"),
    )

    completion_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    household_id: Mapped[str] = mapped_column(String(50), nullable=False)
    appliance_type: Mapped[str] = mapped_column(String(50), nullable=False)
    target_date: Mapped[date] = mapped_column(Date, nullable=False)
    revision: Mapped[int] = mapped_column(Integer, nullable=False)
    input_snapshot_id: Mapped[str] = mapped_column(String(64), nullable=False)
    analysis_run_id: Mapped[str] = mapped_column(String(100), nullable=False)
    analysis_evidence_snapshot_id: Mapped[str] = mapped_column(String(64), nullable=False)
    session_manifest_set_id: Mapped[str] = mapped_column(String(64), nullable=False)
    analysis_status: Mapped[str] = mapped_column(String(20), nullable=False)
    delivery_status: Mapped[str] = mapped_column(String(20), nullable=False)
    coverage_ratio: Mapped[Decimal] = mapped_column(Numeric(7, 6), nullable=False)
    max_unanalyzed_seconds: Mapped[int] = mapped_column(BigInteger, nullable=False)
    quality_policy_version: Mapped[str] = mapped_column(String(50), nullable=False)
    baseline_eligible: Mapped[bool] = mapped_column(Boolean, nullable=False)
    completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


SESSION_LAKE_BATCH_KINDS = ("INITIAL", "INCREMENTAL")
SESSION_LAKE_BATCH_STATUSES = ("ASSIGNED", "FAILED", "COMPLETED")
SESSION_LAKE_OUTBOX_OPERATIONS = ("INSERT", "UPDATE", "DELETE")
SESSION_LAKE_DELIVERY_STATUSES = ("PENDING", "ASSIGNED", "DELIVERED")


def _sql_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{item}'" for item in values)


class SessionLakeBatch(Base):
    """HDFS 적재 배치 한 번의 대상·진행 상태·manifest 위치·오류 기록."""

    __tablename__ = "session_lake_batch"
    __table_args__ = (
        CheckConstraint(
            f"batch_kind IN ({_sql_list(SESSION_LAKE_BATCH_KINDS)})",
            name="batch_kind",
        ),
        CheckConstraint(
            f"status IN ({_sql_list(SESSION_LAKE_BATCH_STATUSES)})",
            name="status",
        ),
        CheckConstraint("event_count >= 0", name="event_count_nonnegative"),
        CheckConstraint("attempt_count >= 0", name="attempt_count_nonnegative"),
        CheckConstraint("schema_version >= 1", name="schema_version_positive"),
        Index("ix_session_lake_batch_status_created", "status", "created_at"),
    )

    batch_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    batch_kind: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    # 배치 생성 시 확정하는 저장 날짜. 재시도해도 경로가 바뀌지 않는다.
    ingest_date: Mapped[date] = mapped_column(Date, nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    # 대상 이벤트: outbox 행이 batch_id로 가리키며, 여기에는 범위와 개수를 남긴다.
    event_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    first_event_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    last_event_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    row_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    file_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    manifest_path: Mapped[str] = mapped_column(String(500), nullable=False)
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_error_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    details: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"),
        nullable=False,
        default=dict,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class SessionLakeOutbox(Base):
    """appliance_usage_session 변경 한 건의 전체 내용과 레이크 전달 상태.

    PostgreSQL 트리거가 세션 변경과 같은 트랜잭션에서 행을 넣는다. 원본 세션에
    대한 FK는 두지 않아 세션이 삭제(CASCADE 포함)된 뒤에도 삭제 이벤트가 남는다.
    """

    __tablename__ = "session_lake_outbox"
    __table_args__ = (
        UniqueConstraint(
            "session_id",
            "session_version",
            name="uq_session_lake_outbox_session_version",
        ),
        CheckConstraint(
            f"operation IN ({_sql_list(SESSION_LAKE_OUTBOX_OPERATIONS)})",
            name="operation",
        ),
        CheckConstraint(
            f"delivery_status IN ({_sql_list(SESSION_LAKE_DELIVERY_STATUSES)})",
            name="delivery_status",
        ),
        CheckConstraint("session_version >= 1", name="session_version_positive"),
        CheckConstraint(
            "(delivery_status = 'PENDING') = (batch_id IS NULL)",
            name="batch_assigned_matches_status",
        ),
        Index(
            "ix_session_lake_outbox_undelivered",
            "delivery_status",
            "event_id",
            postgresql_where=text("delivery_status <> 'DELIVERED'"),
        ),
        Index("ix_session_lake_outbox_batch_id", "batch_id"),
    )

    event_id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        Identity(),
        primary_key=True,
    )
    session_id: Mapped[UUID] = mapped_column(nullable=False)
    session_version: Mapped[int] = mapped_column(Integer, nullable=False)
    operation: Mapped[str] = mapped_column(String(10), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"),
        nullable=False,
    )
    changed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    delivery_status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=text("'PENDING'")
    )
    batch_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("session_lake_batch.batch_id"),
        nullable=True,
    )
    delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


LAKE_RUN_STATUSES = (
    "WAITING_INPUT",
    "RUNNING",
    "VALIDATING",
    "SUCCEEDED",
    "FAILED",
)
LAKE_VERSION_STATUSES = ("ACTIVE", "SUPERSEDED")


class LakeBatchRun(Base):
    """레이크 일일 배치 한 번의 실행.

    batch_run과 달리 입력 스냅샷·규칙·설정 버전을 실행 식별에 포함한다. 날짜별 성공
    여부만으로는 늦게 도착한 데이터나 규칙 변경에 따른 수정 재처리를 표현할 수 없다.
    """

    __tablename__ = "lake_batch_run"
    __table_args__ = (
        UniqueConstraint(
            "job_name",
            "target_date",
            "attempt",
            name="uq_lake_batch_run_job_date_attempt",
        ),
        CheckConstraint(f"status IN ({_sql_list(LAKE_RUN_STATUSES)})", name="status"),
        CheckConstraint("attempt >= 1", name="attempt_positive"),
        Index("ix_lake_batch_run_job_date_status", "job_name", "target_date", "status"),
    )

    run_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    job_name: Mapped[str] = mapped_column(String(50), nullable=False)
    target_date: Mapped[date] = mapped_column(Date, nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    input_snapshot_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    rule_version: Mapped[str] = mapped_column(String(50), nullable=False)
    config_version: Mapped[str] = mapped_column(String(100), nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    details: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"),
        nullable=False,
        server_default=text("'{}'"),
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


class LakeDatasetVersion(Base):
    """날짜별 레이크 데이터셋의 실행 버전과 현재 활성 버전.

    여러 run_id 디렉터리를 통째로 읽으면 같은 날짜가 중복된다. 후속 집계는 여기서
    ACTIVE인 실행만 골라 읽는다. DB가 가리키기 전의 출력은 소비하지 않는다.
    """

    __tablename__ = "lake_dataset_version"
    __table_args__ = (
        UniqueConstraint(
            "dataset_name",
            "target_date",
            "run_id",
            name="uq_lake_dataset_version_dataset_date_run",
        ),
        CheckConstraint(
            f"status IN ({_sql_list(LAKE_VERSION_STATUSES)})", name="status"
        ),
        CheckConstraint("row_count >= 0", name="row_count_nonnegative"),
        Index(
            "ix_lake_dataset_version_active",
            "dataset_name",
            "target_date",
            unique=True,
            postgresql_where=text("status = 'ACTIVE'"),
            sqlite_where=text("status = 'ACTIVE'"),
        ),
    )

    version_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    dataset_name: Mapped[str] = mapped_column(String(64), nullable=False)
    target_date: Mapped[date] = mapped_column(Date, nullable=False)
    run_id: Mapped[UUID] = mapped_column(
        ForeignKey("lake_batch_run.run_id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    output_path: Mapped[str] = mapped_column(Text, nullable=False)
    manifest_path: Mapped[str] = mapped_column(Text, nullable=False)
    row_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    input_snapshot_id: Mapped[str] = mapped_column(String(64), nullable=False)
    rule_version: Mapped[str] = mapped_column(String(50), nullable=False)
    config_version: Mapped[str] = mapped_column(String(100), nullable=False)
    published_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class LakeDatasetDependency(Base):
    """레이크 작업 하나가 소비한 상위 데이터셋 버전.

    상위 날짜가 재처리되면 그 날짜를 소비한 하위 결과는 낡은 것이 된다. 소비 사실을
    활성화와 같은 트랜잭션에 남겨야, 알림을 놓쳐도 "내 활성 결과가 쓴 상위 버전"과
    "지금 활성인 상위 버전"을 비교해 다시 계산할 날짜를 정확히 찾을 수 있다.

    상위 쪽은 FK로 묶지 않는다. 상위 버전 행이 정리된 뒤에도 무엇을 썼는지는 남아야 한다.
    """

    __tablename__ = "lake_dataset_dependency"
    __table_args__ = (
        UniqueConstraint(
            "consumer_run_id",
            "upstream_dataset_name",
            "upstream_target_date",
            name="uq_lake_dataset_dependency_consumer_upstream",
        ),
        Index(
            "ix_lake_dataset_dependency_upstream",
            "upstream_dataset_name",
            "upstream_target_date",
        ),
    )

    dependency_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    consumer_run_id: Mapped[UUID] = mapped_column(
        ForeignKey("lake_batch_run.run_id", ondelete="CASCADE"), nullable=False
    )
    upstream_dataset_name: Mapped[str] = mapped_column(String(64), nullable=False)
    upstream_target_date: Mapped[date] = mapped_column(Date, nullable=False)
    upstream_run_id: Mapped[UUID] = mapped_column(nullable=False)
    upstream_version_id: Mapped[UUID] = mapped_column(nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

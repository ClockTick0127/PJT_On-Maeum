"""Kafka input, model output, baseline, and analysis event schemas."""

from datetime import date, datetime, time
from enum import StrEnum
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# --데이터 형태--

# Kafka 전력 입력
class PowerMeasurement(BaseModel):
    """Canonical fields consumed from power.raw.v1."""

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        allow_inf_nan=False,
    )

    message_id: UUID
    household_id: str = Field(min_length=1, max_length=50)
    device_id: str = Field(min_length=1, max_length=50)
    measured_at: datetime

    active_power: float = Field(ge=0)
    reactive_power: float
    power_factor: float = Field(ge=-1, le=1)
    current: float = Field(ge=0)

    @field_validator("measured_at")
    @classmethod
    def measured_at_must_include_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("measured_at must include a timezone")
        return value


class AnalysisProcessingOutcome(StrEnum):
    """Terminal result for one validated power input."""

    SUCCEEDED = "SUCCEEDED"
    SKIPPED_WARMUP = "SKIPPED_WARMUP"
    SKIPPED_QUALITY_GATE = "SKIPPED_QUALITY_GATE"
    FAILED_INFERENCE = "FAILED_INFERENCE"
    FAILED_PERSISTENCE = "FAILED_PERSISTENCE"


class ProcessingSource(BaseModel):
    """Stable source position used to connect a receipt to Kafka input."""

    model_config = ConfigDict(extra="forbid")

    topic: str | None = None
    partition: int | None = None
    offset: int | None = None


class AppliancePrediction(BaseModel):  # AI 모델이 직접 반환한 원본 결과 
    """One appliance ON probability returned by a Predictor."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    appliance_type: str
    probability: float = Field(ge=0, le=1)


class ApplianceState(BaseModel):  # 분석 서브사 확률과 threshold를 비교해 만든 최종 판정 
    """Thresholded appliance state with the evidence used for the decision."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    appliance_type: str
    probability: float = Field(ge=0, le=1)
    threshold: float = Field(ge=0, le=1)
    is_on: bool


class ApplianceTransitionType(StrEnum):
    TURNED_ON = "TURNED_ON"
    TURNED_OFF = "TURNED_OFF"


class ApplianceStateTransition(BaseModel):
    """A debounced appliance state transition ready for session processing."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    household_id: str = Field(min_length=1, max_length=50)
    appliance_type: str
    transition_type: ApplianceTransitionType
    previous_is_on: bool
    current_is_on: bool
    occurred_at: datetime
    confirmed_at: datetime
    probability: float = Field(ge=0, le=1)
    threshold: float = Field(ge=0, le=1)


SNAPSHOT_APPLIANCE_ORDER = (
    "KETTLE",
    "INDUCTION",
    "IRON",
    "MICROWAVE",
    "HAIR_DRYER",
    "VACUUM_CLEANER",
)


class SnapshotMeasurement(BaseModel):
    """Snapshot에 포함하는 최신 원본 전력 측정값."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    active_power: float = Field(ge=0)
    reactive_power: float
    power_factor: float = Field(ge=-1, le=1)
    current: float = Field(ge=0)


class SnapshotApplianceState(BaseModel):
    """연속 판정과 히스테리시스를 적용한 가전의 최종 상태."""

    model_config = ConfigDict(extra="forbid")

    appliance_type: str
    is_on: bool


class AnalysisSnapshot(BaseModel):
    """analysis.snapshot.v1으로 발행하는 가구별 최신 상태."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    schema_version: Literal[1] = 1
    snapshot_id: UUID
    household_id: str = Field(min_length=1, max_length=50)
    observed_at: datetime
    published_at: datetime
    measurement: SnapshotMeasurement
    appliances: list[SnapshotApplianceState]

    @field_validator("observed_at", "published_at")
    @classmethod
    def snapshot_times_must_include_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("snapshot timestamps must include a timezone")
        return value

    @model_validator(mode="after")
    def snapshot_must_contain_six_appliances(self) -> "AnalysisSnapshot":
        actual = tuple(appliance.appliance_type for appliance in self.appliances)
        if actual != SNAPSHOT_APPLIANCE_ORDER:
            raise ValueError(
                "snapshot appliances must contain the six agreed appliance types "
                "in contract order"
            )
        return self


class RoutineBaseline(BaseModel):
    id: UUID
    household_id: str
    appliance_type: str
    baseline_type: str = "ROUTINE_MISSED"
    expected_until: time
    normal_days: int = Field(ge=0)
    window_days: int = Field(gt=0)
    reliability_weight: float = Field(default=1.0, ge=0, le=1)
    enabled: bool = True

    @model_validator(mode="after")
    def normal_days_cannot_exceed_window(self) -> "RoutineBaseline":
        if self.normal_days > self.window_days:
            raise ValueError("normal_days cannot exceed window_days")
        return self


class AnalysisPolicyDefinition(BaseModel):
    """Bootstrap and runtime shape of an enabled anomaly policy."""

    model_config = ConfigDict(extra="forbid")

    policy_code: str = Field(min_length=1, max_length=50)
    algorithm_type: str = Field(min_length=1, max_length=50)
    parameters: dict[str, Any]
    event_type: Literal[
        "ROUTINE_MISSED",
        "PROLONGED_INACTIVITY",
        "PROLONGED_APPLIANCE_USE",
        "ROUTINE_CHANGED",
    ]
    cooldown_hours: int = Field(default=0, ge=0)
    enabled: bool = True


# 이상 이벤트 형식 
class AnalysisEvent(BaseModel):
    """MVP contract published to analysis.event.v1."""

    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    household_id: str = Field(min_length=1, max_length=50)
    event_type: Literal[
        "ROUTINE_MISSED",
        "PROLONGED_INACTIVITY",
        "PROLONGED_APPLIANCE_USE",
        "ROUTINE_CHANGED",
    ]
    occurred_at: datetime
    reason: dict[str, Any]

    @field_validator("occurred_at")
    @classmethod
    def event_time_must_include_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include a timezone")
        return value


class ActivityIndexComponents(BaseModel):
    """Inputs and normalized component scores used for a daily activity index."""

    model_config = ConfigDict(extra="forbid")

    usage_count: int = Field(ge=0)
    appliance_type_count: int = Field(ge=0, le=len(SNAPSHOT_APPLIANCE_ORDER))
    usage_duration_seconds: int = Field(ge=0)
    usage_count_score: int = Field(ge=0, le=100)
    appliance_diversity_score: int = Field(ge=0, le=100)
    usage_duration_score: int = Field(ge=0, le=100)


class ActivityIndexMessage(BaseModel):
    """Daily activity index contract published to analysis.activity.v1."""

    model_config = ConfigDict(extra="forbid")

    message_id: UUID
    household_id: str = Field(min_length=1, max_length=50)
    activity_date: date
    activity_index: int | None = Field(default=None, ge=0, le=100)
    data_status: Literal["VALID", "INSUFFICIENT_DATA"]
    components: ActivityIndexComponents | None = None

    @model_validator(mode="after")
    def index_and_components_must_match_status(self) -> "ActivityIndexMessage":
        if self.data_status == "VALID":
            if self.activity_index is None or self.components is None:
                raise ValueError(
                    "VALID activity data requires activity_index and components"
                )
        elif self.activity_index is not None:
            raise ValueError("non-VALID activity data must not contain activity_index")
        return self


class DataQualityEvent(BaseModel):
    """Data availability contract published to analysis.data-quality.v1."""

    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    household_id: str = Field(min_length=1, max_length=50)
    event_type: Literal["DATA_GAP", "DATA_RECOVERED"]
    occurred_at: datetime
    reason: dict[str, Any]

    @field_validator("occurred_at")
    @classmethod
    def data_quality_time_must_include_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include a timezone")
        return value


class OutingEvent(BaseModel):
    """Household outing contract consumed from monitoring."""

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    event_id: UUID
    household_id: str = Field(min_length=1, max_length=50)
    event_type: Literal["OUTING_STARTED", "OUTING_ENDED"]
    occurred_at: datetime

    @field_validator("occurred_at")
    @classmethod
    def outing_time_must_include_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include a timezone")
        return value


class DlqMessage(BaseModel):
    source_topic: str
    source_partition: int
    source_offset: int
    error_code: str
    error_message: str
    failed_at: datetime
    payload: Any

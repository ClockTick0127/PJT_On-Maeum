"""Kafka input, model output, baseline, and analysis event schemas."""

from datetime import datetime, time
from typing import Any
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


class ApplianceState(BaseModel):
    appliance_type: str
    is_on: bool


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


# 이상 이벤트 형식 
class AnalysisEvent(BaseModel):
    """MVP contract published to analysis.event.v1."""

    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    household_id: str = Field(min_length=1, max_length=50)
    score: int = Field(ge=0, le=100)
    occurred_at: datetime
    reason: dict[str, Any]


class DlqMessage(BaseModel):
    source_topic: str
    source_partition: int
    source_offset: int
    error_code: str
    error_message: str
    failed_at: datetime
    payload: Any

"""Wire contracts only; no model, database, broker or service side effects."""
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

ORDER = ('KETTLE', 'INDUCTION', 'IRON', 'MICROWAVE', 'HAIR_DRYER', 'VACUUM_CLEANER')
Appliance = Literal['KETTLE', 'INDUCTION', 'IRON', 'MICROWAVE', 'HAIR_DRYER', 'VACUUM_CLEANER']
Identifier = Annotated[str, Field(min_length=1, max_length=100, pattern=r'^\S+$')]
Household = Annotated[str, Field(min_length=1, max_length=50, pattern=r'^\S+$')]
Index = Annotated[int, Field(ge=0)]
Probability = Annotated[float, Field(ge=0, le=1)]
Sha = Annotated[str, Field(pattern=r'^[0-9a-f]{64}$')]


class Contract(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True, allow_inf_nan=False)


class Features(Contract):
    active_power: Annotated[float, Field(ge=0)]
    reactive_power: float
    power_factor: Annotated[float, Field(ge=-1, le=1)]
    current: Annotated[float, Field(ge=0)]


class Measurement(Contract):
    # Existing simulator messages contain additional legacy fields. Ignore them
    # just as PowerMeasurement does; never derive features from legacy fields.
    model_config = ConfigDict(extra='ignore', strict=True, allow_inf_nan=False)
    message_id: UUID
    household_id: Household
    device_id: Household
    measured_at: AwareDatetime
    run_id: Identifier
    profile_id: Identifier
    source_index: Index
    valid: bool
    context: bool
    active_power: Annotated[float, Field(ge=0)] | None = None
    reactive_power: float | None = None
    power_factor: Annotated[float, Field(ge=-1, le=1)] | None = None
    current: Annotated[float, Field(ge=0)] | None = None

    @model_validator(mode='after')
    def required_features(self):
        if self.valid and any(getattr(self, field) is None for field in Features.model_fields):
            raise ValueError('valid input requires all four raw features')
        return self


class Runtime(Contract):
    profile_id: Identifier
    checkpoint_sha256: Sha
    model_code_sha256: Sha
    normalization_sha256: Sha
    torch_version: Annotated[str, Field(min_length=1)]
    device: Annotated[str, Field(min_length=1)]
    dtype: Literal['float32', 'bfloat16']
    batch_size: Literal[1]
    input_shape: tuple[Literal[1], Literal[255], Literal[4]]
    reference_device: str
    reference_dtype: str
    reference_parity_verified: bool


class Source(Contract):
    topic: Identifier
    partition: Index
    offset: Index


class ApplianceState(Contract):
    appliance_type: Appliance
    inferred: bool
    probability: Probability | None
    state: Literal['ON', 'OFF', 'UNKNOWN']
    is_on: bool | None

    @model_validator(mode='after')
    def state_consistency(self):
        if not self.inferred:
            if self.state != 'UNKNOWN' or self.probability is not None or self.is_on is not None:
                raise ValueError('not inferred must remain UNKNOWN/null/null')
        elif self.probability is None:
            raise ValueError('inferred state requires a finite probability')
        expected = None if self.state == 'UNKNOWN' else self.state == 'ON'
        if self.is_on is not expected:
            raise ValueError('state and is_on disagree')
        return self


class Snapshot(Contract):
    schema_version: Literal[2]
    scope: Literal['selected_scene']
    snapshot_id: UUID
    household_id: Household
    run_id: Identifier
    profile_id: Identifier
    source_index: Index
    observed_at: AwareDatetime
    published_at: AwareDatetime
    target_appliance: Appliance
    ready: bool
    transition: Literal['SYNC', 'TURNED_ON', 'TURNED_OFF'] | None
    measurement: Features | None
    source: Source | None
    runtime: Runtime
    appliances: Annotated[list[ApplianceState], Field(min_length=6, max_length=6)]

    @model_validator(mode='after')
    def inference_scope(self):
        if tuple(a.appliance_type for a in self.appliances) != ORDER:
            raise ValueError('six appliances must appear in contract order')
        if self.runtime.profile_id != self.profile_id:
            raise ValueError('runtime profile mismatch')
        for a in self.appliances:
            if a.inferred != (self.ready and a.appliance_type == self.target_appliance):
                raise ValueError('only the ready target can be inferred')
        target = next(a for a in self.appliances if a.appliance_type == self.target_appliance)
        if self.transition is not None and (not self.ready or target.state == 'UNKNOWN'):
            raise ValueError('unknown/unready state cannot emit a transition')
        if self.transition == 'TURNED_ON' and target.state != 'ON':
            raise ValueError('TURNED_ON must end in ON')
        if self.transition == 'TURNED_OFF' and target.state != 'OFF':
            raise ValueError('TURNED_OFF must end in OFF')
        return self


class Session(Contract):
    schema_version: Literal[1]
    scope: Literal['selected_scene']
    event_id: UUID
    session_id: UUID
    revision: Annotated[int, Field(ge=1)]
    household_id: Household
    run_id: Identifier
    profile_id: Identifier
    appliance_type: Appliance
    source_index: Index
    status: Literal['OPEN', 'CLOSED', 'INTERRUPTED']
    start_known: bool
    started_at: AwareDatetime | None
    observed_start_at: AwareDatetime
    observed_until_at: AwareDatetime
    ended_at: AwareDatetime | None
    end_reason: Literal['TURNED_OFF', 'GAP', 'EOF', 'CANCELLED'] | None
    observed_on_seconds: Index

    @model_validator(mode='after')
    def session_consistency(self):
        if self.start_known != (self.started_at is not None):
            raise ValueError('unknown start must use started_at=null')
        if self.started_at is not None and self.started_at != self.observed_start_at:
            raise ValueError('known start must be the observed ON transition')
        if self.observed_until_at < self.observed_start_at:
            raise ValueError('session observations must be chronological')
        if self.observed_on_seconds > (self.observed_until_at - self.observed_start_at).total_seconds():
            raise ValueError('ON duration cannot exceed the observed interval')
        if self.status == 'CLOSED':
            if self.end_reason != 'TURNED_OFF' or self.ended_at != self.observed_until_at:
                raise ValueError('only observed OFF closes a session')
        elif self.ended_at is not None:
            raise ValueError('unobserved physical end must remain null')
        if self.status == 'OPEN' and self.end_reason is not None:
            raise ValueError('open session has no end reason')
        if self.status == 'INTERRUPTED' and self.end_reason not in ('GAP', 'EOF', 'CANCELLED'):
            raise ValueError('interruption requires a non-OFF reason')
        return self


class RiskReason(Contract):
    scope: Literal['selected_scene']
    run_id: Identifier
    profile_id: Identifier
    appliance_type: Appliance
    session_id: UUID
    source_index: Index
    policy_id: Identifier
    policy_scope: Literal['test_household']
    session_started_at: AwareDatetime
    continuous_on_seconds: Annotated[int, Field(ge=1)]
    threshold_seconds: Annotated[int, Field(ge=1)]


class RiskEvent(Contract):
    # Same outer shape as existing AnalysisEvent; restricted to the one rule
    # supported by partial-household observation in this contract revision.
    event_id: UUID
    household_id: Household
    event_type: Literal['PROLONGED_APPLIANCE_USE']
    occurred_at: AwareDatetime
    reason: RiskReason

    @model_validator(mode='after')
    def supported_risk(self):
        if self.reason.continuous_on_seconds < self.reason.threshold_seconds:
            raise ValueError('risk threshold has not been reached')
        if (self.occurred_at - self.reason.session_started_at).total_seconds() != self.reason.continuous_on_seconds:
            raise ValueError('duration must use event time from a known session start')
        return self


MODELS = {'measurement': Measurement, 'snapshot': Snapshot, 'session': Session, 'risk': RiskEvent}

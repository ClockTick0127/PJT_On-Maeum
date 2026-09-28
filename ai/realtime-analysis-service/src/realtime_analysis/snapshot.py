"""가구별 최신 분석 상태를 Snapshot 계약으로 변환한다."""

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import AbstractSet

from realtime_analysis.schemas import (
    AnalysisSnapshot,
    ApplianceState,
    PowerMeasurement,
    SNAPSHOT_APPLIANCE_ORDER,
    SnapshotApplianceState,
    SnapshotMeasurement,
)


def create_analysis_snapshot(
    measurement: PowerMeasurement,
    states: Sequence[ApplianceState],
    active_appliance_types: AbstractSet[str],
    published_at: datetime | None = None,
) -> AnalysisSnapshot:
    """현재 측정값과 히스테리시스 적용 상태를 Snapshot으로 만든다."""

    state_types = {state.appliance_type for state in states}
    contract_types = set(SNAPSHOT_APPLIANCE_ORDER)
    if len(states) != len(SNAPSHOT_APPLIANCE_ORDER) or state_types != contract_types:
        raise ValueError("snapshot requires states for all six appliance types")
    if not active_appliance_types <= contract_types:
        raise ValueError("snapshot contains an unknown active appliance type")

    # 입력 message_id를 Snapshot ID로 재사용해 같은 입력이 재처리돼도 ID를 유지한다.
    return AnalysisSnapshot(
        snapshot_id=measurement.message_id,
        household_id=measurement.household_id,
        observed_at=measurement.measured_at.astimezone(timezone.utc),
        published_at=published_at or datetime.now(timezone.utc),
        measurement=SnapshotMeasurement(
            active_power=measurement.active_power,
            reactive_power=measurement.reactive_power,
            power_factor=measurement.power_factor,
            current=measurement.current,
        ),
        appliances=[
            SnapshotApplianceState(
                appliance_type=appliance_type,
                # threshold 결과가 아니라 연속 판정과 히스테리시스가 끝난 상태다.
                is_on=appliance_type in active_appliance_types,
            )
            for appliance_type in SNAPSHOT_APPLIANCE_ORDER
        ],
    )

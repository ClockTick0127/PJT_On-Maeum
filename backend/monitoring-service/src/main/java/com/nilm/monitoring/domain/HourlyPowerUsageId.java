package com.nilm.monitoring.domain;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Objects;

public class HourlyPowerUsageId implements Serializable {

    private String householdId;
    private OffsetDateTime bucketStartAt; // UTC 정각으로 통일하고 화면에서 한국 시간으로 표시

    public HourlyPowerUsageId() {
    }

    public HourlyPowerUsageId(
            String householdId,
            OffsetDateTime bucketStartAt
    ) {
        this.householdId = householdId;
        this.bucketStartAt = bucketStartAt;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof HourlyPowerUsageId other)) {
            return false;
        }
        return Objects.equals(householdId, other.householdId)
                && Objects.equals(bucketStartAt, other.bucketStartAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(householdId, bucketStartAt);
    }
}
package com.nilm.monitoring.domain;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Objects;

public class HourlyAppliancePowerUsageId implements Serializable {

    private String householdId;
    private OffsetDateTime bucketStartAt; // UTC 정각으로 통일하고 화면에서 한국 시간으로 표시
    private String applianceType;

    public HourlyAppliancePowerUsageId() {
    }

    public HourlyAppliancePowerUsageId(
            String householdId,
            OffsetDateTime bucketStartAt,
            String applianceType
    ) {
        this.householdId = householdId;
        this.bucketStartAt = bucketStartAt;
        this.applianceType = applianceType;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof HourlyAppliancePowerUsageId other)) {
            return false;
        }
        return Objects.equals(householdId, other.householdId)
                && Objects.equals(bucketStartAt, other.bucketStartAt)
                && Objects.equals(applianceType, other.applianceType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(householdId, bucketStartAt, applianceType);
    }
}

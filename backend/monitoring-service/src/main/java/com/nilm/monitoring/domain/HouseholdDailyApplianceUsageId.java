package com.nilm.monitoring.domain;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Objects;

public class HouseholdDailyApplianceUsageId implements Serializable {

    private String householdId;
    private String applianceType;
    private LocalDate usageDate;

    public HouseholdDailyApplianceUsageId() {
    }

    public HouseholdDailyApplianceUsageId(
            String householdId,
            String applianceType,
            LocalDate usageDate
    ) {
        this.householdId = householdId;
        this.applianceType = applianceType;
        this.usageDate = usageDate;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof HouseholdDailyApplianceUsageId other)) {
            return false;
        }
        return Objects.equals(householdId, other.householdId)
                && Objects.equals(applianceType, other.applianceType)
                && Objects.equals(usageDate, other.usageDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(householdId, applianceType, usageDate);
    }
}

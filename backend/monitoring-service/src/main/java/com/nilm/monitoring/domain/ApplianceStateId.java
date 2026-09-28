package com.nilm.monitoring.domain;

import java.io.Serializable;
import java.util.Objects;

public class ApplianceStateId implements Serializable {

    private String householdId;
    private String applianceType;

    public ApplianceStateId() {
    }

    public ApplianceStateId(String householdId, String applianceType) {
        this.householdId = householdId;
        this.applianceType = applianceType;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ApplianceStateId other)) {
            return false;
        }
        return Objects.equals(householdId, other.householdId)
                && Objects.equals(applianceType, other.applianceType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(householdId, applianceType);
    }
}

package com.nilm.monitoring.dto.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;

/**
 * analysis.snapshot.v1 메시지.
 * snapshot_id는 계약상 UUID지만 형식이 어긋난 값이 와도 소비가 멈추지 않도록 문자열로 받는다.
 */
public record AnalysisSnapshotMessage(

        @JsonProperty("schema_version")
        Integer schemaVersion,

        @JsonProperty("snapshot_id")
        String snapshotId,

        @JsonProperty("household_id")
        String householdId,

        @JsonProperty("observed_at")
        OffsetDateTime observedAt,

        @JsonProperty("published_at")
        OffsetDateTime publishedAt,

        Measurement measurement,

        List<Appliance> appliances

) {

    /** 마지막 활동 판정에는 쓰지 않지만 계약을 그대로 담아 둔다. */
    public record Measurement(

            @JsonProperty("active_power")
            BigDecimal activePower,

            @JsonProperty("reactive_power")
            BigDecimal reactivePower,

            @JsonProperty("power_factor")
            BigDecimal powerFactor,

            BigDecimal current
    ) {
    }

    public record Appliance(

            @JsonProperty("appliance_type")
            String applianceType,

            @JsonProperty("is_on")
            Boolean isOn
    ) {
    }
}

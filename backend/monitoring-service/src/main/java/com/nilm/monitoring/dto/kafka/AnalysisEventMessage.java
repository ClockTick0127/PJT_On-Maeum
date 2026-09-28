package com.nilm.monitoring.dto.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

public record AnalysisEventMessage(

        @JsonProperty("event_id")
        UUID eventId,

        @JsonProperty("household_id")
        String householdId,

        Integer score,

        @JsonProperty("occurred_at")
        OffsetDateTime occurredAt,

        Map<String, Object> reason,

        @JsonProperty("event_type") String eventType,
        @JsonProperty("appliance_type") String applianceType

) {
}

package com.nilm.monitoring.dto.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.nilm.monitoring.config.enums.OutingEventType;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * {@code monitoring.household-presence.v1}로 발행하는 외출 이벤트.
 *
 * <p>AI 분석 서비스는 정의되지 않은 필드가 섞이면 계약 오류로 DLQ에 보내므로
 * 네 필드 외에는 아무것도 싣지 않는다. {@code occurredAt}은 발행 시각이 아니라
 * 외출 상태가 실제로 바뀐 시각이고 오프셋을 포함해야 한다.
 */
public record OutingEventMessage(

        @JsonProperty("event_id")
        UUID eventId,

        @JsonProperty("household_id")
        String householdId,

        @JsonProperty("event_type")
        OutingEventType eventType,

        @JsonProperty("occurred_at")
        OffsetDateTime occurredAt
) {
}

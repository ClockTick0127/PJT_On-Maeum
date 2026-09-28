package com.nilm.monitoring.service;

import com.nilm.monitoring.config.enums.OutingEventType;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * 가구의 외출 상태가 실제로 뒤집혔다는 내부 신호.
 *
 * 커밋된 뒤에 Kafka로 내보내려고 쓴다. {@code eventId}는 상태가 바뀐 시점에
 * 한 번만 만들어 두므로, 발행을 재시도해도 AI가 같은 이벤트로 알아본다.
 */
public record HouseholdPresenceChanged(
        UUID eventId,
        String householdId,
        OutingEventType eventType,
        OffsetDateTime occurredAt
) {
}

package com.nilm.monitoring.dto.kafka;

import java.time.OffsetDateTime;

/**
 * device-service가 MQTT LWT로 감지한 기기 접속 상태 변화.
 * 토픽: device.connection-changed.v1
 */
public record DeviceConnectionChangedMessage(
        Long deviceId,
        String houseId,
        String status,
        OffsetDateTime occurredAt) {
}

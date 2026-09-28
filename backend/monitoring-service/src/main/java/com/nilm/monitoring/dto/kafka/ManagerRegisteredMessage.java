package com.nilm.monitoring.dto.kafka;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * iot-device-service가 보내는 담당자 가입 이벤트(device.manager-registered.v1).
 *
 * <p>{@code authSub}는 Keycloak 사용자 ID이고, 담당자 행의 auth_sub와 같은 값이다.
 * 로그인한 담당자를 찾는 기준이 이 값이므로 비어 있으면 쓸 수 없는 메시지다.
 */
public record ManagerRegisteredMessage(
        UUID eventId,
        String authSub,
        String name,
        String organization,
        String phone,
        String email,
        OffsetDateTime occurredAt
) {
}

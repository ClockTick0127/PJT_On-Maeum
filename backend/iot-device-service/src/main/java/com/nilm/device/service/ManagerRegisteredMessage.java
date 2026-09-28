package com.nilm.device.service;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * 담당자 가입 이벤트 계약(device.manager-registered.v1).
 *
 * <p>{@code authSub}는 Keycloak 사용자 ID다. 받는 쪽은 이 값으로 자기 담당자 행을
 * 찾으므로, 같은 이벤트를 두 번 받아도 한 명으로 수렴한다.
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

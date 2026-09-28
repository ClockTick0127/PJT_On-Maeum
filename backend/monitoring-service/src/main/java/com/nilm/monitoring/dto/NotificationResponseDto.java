package com.nilm.monitoring.dto;

import java.time.OffsetDateTime;

public record NotificationResponseDto(
        String id,
        String status,
        OffsetDateTime respondedAt,
        String responseSource
) {
}
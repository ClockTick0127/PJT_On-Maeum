package com.nilm.monitoring.dto;

import com.nilm.monitoring.domain.Notification;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

/**
 * 담당자가 알림을 어디까지 처리했는지 바꾸는 요청.
 *
 * <p>RESOLVED로 바꾸면 그 이벤트가 세운 위험 등급도 함께 해제된다.
 */
public record ManagerStatusRequest(

        @Schema(description = "담당자 처리 상태", example = "RESOLVED")
        @NotNull(message = "처리 상태가 필요합니다.")
        Notification.ManagerResponseStatus status
) {
}

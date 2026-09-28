package com.nilm.monitoring.api;

import com.nilm.monitoring.dto.ManagerStatusRequest;
import com.nilm.monitoring.dto.NotificationResponseDto;
import com.nilm.monitoring.dto.NotificationResponseRequest;
import com.nilm.monitoring.service.NotificationService;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/monitoring/notifications")
public class NotificationController {

    private final NotificationService service;

    @Operation(
            summary = "안전 확인 응답",
            description = "대상자 본인이 안전 확인 알림에 답한다. 응답 기한이 지나면 거부된다."
    )
    @PutMapping("/{notificationId}/responses")
    public NotificationResponseDto respond(
            @AuthenticationPrincipal Jwt jwt,
            @PathVariable("notificationId") Long notificationId,
            @Valid @RequestBody NotificationResponseRequest request
    ) {
        String authSub = jwt == null ? null : jwt.getSubject();
        return service.respond(notificationId, authSub, request);
    }

    @Operation(
            summary = "담당자 처리 상태 변경",
            description = """
                    배정된 담당자가 알림의 처리 상태를 바꾼다.
                    RESOLVED로 바꾸면 그 이벤트가 세운 위험 등급 슬롯도 함께 해제되고,
                    유효 등급은 모니터링 자체 평가 등급으로 되돌아간다.
                    """
    )
    @PutMapping("/{notificationId}/manager-status")
    public ResponseEntity<Void> changeManagerStatus(
            @AuthenticationPrincipal Jwt jwt,
            @PathVariable("notificationId") Long notificationId,
            @Valid @RequestBody ManagerStatusRequest request
    ) {
        String authSub = jwt == null ? null : jwt.getSubject();
        service.changeManagerStatus(notificationId, authSub, request.status());
        return ResponseEntity.noContent().build();
    }
}

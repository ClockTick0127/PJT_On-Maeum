package com.nilm.monitoring.api;

import com.nilm.monitoring.dto.AwayModeRequest;
import com.nilm.monitoring.dto.MyDashboardResponse;
import com.nilm.monitoring.dto.SubjectMonitoringResponse;
import com.nilm.monitoring.service.AwayModeService;
import com.nilm.monitoring.service.MyDashboardService;
import com.nilm.monitoring.service.SubjectMonitoringService;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/monitoring")
public class MonitoringDashboardController {

    private final SubjectMonitoringService subjectMonitoringService;
    private final MyDashboardService myDashboardService;
    private final AwayModeService awayModeService;

    @Operation(
            summary = "담당자 대시보드 목록",
            description = """
                    배정된 대상자의 현재 상태를 한 번에 내려준다.
                    riskLevel은 자체 평가 등급과 이벤트 등급 중 높은 쪽이고,
                    riskSource가 어느 쪽인지 알려 준다.
                    assessmentStatus가 VALID가 아니면 마지막으로 성립한 평가 값을 그대로 보여준다.
                    riskTrend는 분석 이벤트 점수와 자체 평가 점수의 일별 최댓값이다.
                    """
    )
    @GetMapping("/dashboard")
    public ResponseEntity<SubjectMonitoringResponse> dashboard(
            @AuthenticationPrincipal Jwt jwt
    ) {
        String authSub = jwt == null ? null : jwt.getSubject();
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(subjectMonitoringService.getSubjects(authSub));
    }

    @GetMapping("/my-dashboard")
    public ResponseEntity<MyDashboardResponse> myDashboard(
            @AuthenticationPrincipal Jwt jwt
    ) {
        String authSub = jwt == null ? null : jwt.getSubject();
        return ResponseEntity.ok(myDashboardService.getMyDashboard(authSub));
    }

    /**
     * 외출 모드 설정. startsAt을 생략하면 즉시, endsAt을 생략하면 해제할 때까지 이어진다.
     * enabled=false는 해제이며 시간을 함께 보낼 수 없다.
     */
    @PutMapping("/my-dashboard/away-mode")
    public ResponseEntity<Void> updateAwayMode(
            @AuthenticationPrincipal Jwt jwt,
            @Valid @RequestBody AwayModeRequest request
    ) {
        String authSub = jwt == null ? null : jwt.getSubject();
        awayModeService.updateAwayMode(authSub, request);
        return ResponseEntity.noContent().build();
    }
}

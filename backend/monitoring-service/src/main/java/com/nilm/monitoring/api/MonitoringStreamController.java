package com.nilm.monitoring.api;

import com.nilm.monitoring.service.SubjectStatusStreamService;
import io.swagger.v3.oas.annotations.Operation;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * 담당자 대시보드 실시간 조회.
 * 토큰 주인이 담당자이고, 그 담당자에게 배정된 대상자 전체의 변화만 흘러나온다.
 */
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/monitoring")
public class MonitoringStreamController {

    private final SubjectStatusStreamService streamService;

    @Operation(
            summary = "담당자 대시보드 실시간 스트림",
            description = """
                    subject-status 이벤트로 바뀐 대상자 1명의 현재 상태를 내려준다.
                    trigger가 갱신 이유를 알려 주며, ASSESSMENT는 모니터링 자체 평가가 반영된 경우다.
                    riskLevel은 자체 평가 등급과 이벤트 등급 중 높은 쪽이고 riskSource로 출처를 구분한다.
                    같은 평가 결과가 반복되는 동안에는 이벤트를 보내지 않는다.
                    """
    )
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(@AuthenticationPrincipal Jwt jwt) {
        String authSub = jwt == null ? null : jwt.getSubject();
        return streamService.subscribe(authSub);
    }
}

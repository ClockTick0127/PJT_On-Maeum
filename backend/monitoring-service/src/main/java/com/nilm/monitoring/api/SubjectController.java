package com.nilm.monitoring.api;

import com.nilm.monitoring.dto.SubjectCreateRequest;
import com.nilm.monitoring.dto.SubjectEventsResponse;
import com.nilm.monitoring.dto.SubjectMonitoringResponse;
import com.nilm.monitoring.dto.SubjectPowerUsageResponse;
import com.nilm.monitoring.dto.SubjectProfileResponse;
import com.nilm.monitoring.service.HouseholdProfileService;
import com.nilm.monitoring.service.SubjectEventService;
import com.nilm.monitoring.service.SubjectPowerUsageService;
import com.nilm.monitoring.service.SubjectRegistrationService;
import com.nilm.monitoring.service.SubjectMonitoringService;
import jakarta.validation.Valid;
import java.time.LocalDate;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/monitoring/subjects")
public class SubjectController {

    private final SubjectRegistrationService registrationService;
    private final SubjectMonitoringService monitoringService;
    private final SubjectPowerUsageService powerUsageService;
    private final SubjectEventService eventService;
    private final HouseholdProfileService householdProfileService;

    @GetMapping("/search")
    public ResponseEntity<SubjectMonitoringResponse> getSubjects(
            @AuthenticationPrincipal Jwt jwt
    ) {
        String authSub = jwt == null ? null : jwt.getSubject();
        return ResponseEntity
                .status(HttpStatus.OK)
                .body(monitoringService.getSubjects(authSub));
    }

    /**
     * 대상자 상세 화면의 하루 전력 사용 패턴 그래프.
     * date를 생략하면 Asia/Seoul 기준 오늘을 조회한다.
     *
     * <p>구간의 {@code status}는 시각뿐 아니라 그 시간을 실제로 관측했는지도 함께 본다.
     * 스냅샷이 끊겨 본 적 없는 구간은 {@code NO_DATA}이고 {@code usage}가 null이다.
     * 전기를 안 쓴 시간({@code COMPLETE} + 0)과 구분해서 그려야 한다.
     */
    @GetMapping("/{subjectId}/power-usage")
    public ResponseEntity<SubjectPowerUsageResponse> getPowerUsage(
            @AuthenticationPrincipal Jwt jwt,
            @PathVariable Long subjectId,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date
    ) {
        String authSub = jwt == null ? null : jwt.getSubject();
        return ResponseEntity
                .status(HttpStatus.OK)
                .body(powerUsageService.getPowerUsage(authSub, subjectId, date));
    }

    /**
     * 대상자 상세 화면의 이상 징후(위험 이벤트) 기록.
     * from/to를 생략하면 Asia/Seoul 기준 최근 7일을 조회한다.
     */
    @GetMapping("/{subjectId}/events")
    public ResponseEntity<SubjectEventsResponse> getEvents(
            @AuthenticationPrincipal Jwt jwt,
            @PathVariable Long subjectId,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to,
            @RequestParam(required = false) Integer size,
            @RequestParam(required = false) String cursor
    ) {
        String authSub = jwt == null ? null : jwt.getSubject();
        return ResponseEntity
                .status(HttpStatus.OK)
                .body(eventService.getEvents(
                        authSub, subjectId, from, to, size, cursor));
    }

    /**
     * 대상자 상세 화면에서 지금 반영된 생활 프로필을 확인한다.
     * Gold 배치가 아직 프로필을 보내지 않았으면 204로 답한다.
     */
    @GetMapping("/{subjectId}/profile")
    public ResponseEntity<SubjectProfileResponse> getProfile(
            @AuthenticationPrincipal Jwt jwt,
            @PathVariable Long subjectId
    ) {
        String authSub = jwt == null ? null : jwt.getSubject();
        return householdProfileService.getProfile(authSub, subjectId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.noContent().build());
    }

    @PostMapping
    public ResponseEntity<Void> register(
            @AuthenticationPrincipal Jwt jwt,
            @Valid @RequestBody SubjectCreateRequest request
    ) {
        // jwt.getSubject(): 담당자 로그인 ID
        if (jwt == null
                || jwt.getSubject() == null
                || jwt.getSubject().isBlank()) {
            throw new ResponseStatusException(
                    HttpStatus.UNAUTHORIZED,
                    "로그인이 필요합니다."
            );
        }

        registrationService.register(
                jwt.getSubject(),
                request
        );

        // 응답 Body 없이 상태 코드만 반환한다.
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .build();
    }

}

package com.nilm.monitoring.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nilm.monitoring.config.EventRoutingPolicy;
import com.nilm.monitoring.config.RiskProperties;
import com.nilm.monitoring.config.enums.RiskLevel;
import com.nilm.monitoring.config.enums.StateChangeTrigger;
import com.nilm.monitoring.domain.AnalysisEvent;
import com.nilm.monitoring.domain.Notification;
import com.nilm.monitoring.domain.NotificationSetting;
import com.nilm.monitoring.domain.Subject;
import com.nilm.monitoring.dto.kafka.AnalysisEventMessage;
import com.nilm.monitoring.repository.AnalysisEventRepository;
import com.nilm.monitoring.repository.SubjectRepository;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 분석 서비스 이벤트를 유형별 라우팅 정책에 따라 처리한다(설계 11.4절).
 *
 * <p>예전에는 모든 이벤트가 한 흐름을 탔다. 수신 {@code score}로 등급을 매기고,
 * 외출 중이면 무조건 알림을 생략했다. 두 가정이 모두 틀렸다.
 * 분석 서비스의 이벤트 계약에는 {@code score}가 없고, 장시간 사용은 부재 중일수록 위험하다.
 *
 * <p>이제 이 서비스는 "이벤트를 어디까지 반영할지"만 정한다. 최종 위험 점수의 주체는
 * {@link RiskAssessmentService} 하나이며, 여기서 세우는 등급은 자체 평가와 섞이지 않는
 * 별도 슬롯에 들어간다.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class AnalysisEventService {

    private final SubjectRepository subjects;
    private final AnalysisEventRepository events;
    private final NotificationService notifications;
    private final NotificationGate notificationGate;
    private final AnalysisEventNarrator narrator;
    private final EventRoutingPolicy routing;
    private final DeviceConnectivityService deviceConnectivity;
    private final RiskProperties riskProperties;
    private final ApplicationEventPublisher publisher;
    private final ObjectMapper objectMapper;

    @Value("${app.push.enabled:false}")
    private boolean pushEnabled;

    /**
     * Kafka Consumer에서 넘어온 이벤트 한 건을 반영한다.
     */
    @Transactional
    public void handle(AnalysisEventMessage message) {
        validate(message);

        // 이벤트 토픽에는 이 서비스가 모르는 가구(시뮬레이터 테스트 가구 등)도 흘러온다.
        // 예외로 컨슈머를 멈추면 다른 가구의 알림까지 끊기므로 경고만 남기고 건너뛴다.
        // 이 로그가 곧 "알림이 안 온 이유"이므로 INFO 이상으로 남긴다.
        // household_id는 유니크라 결과는 0건 또는 1건이다.
        var matches = subjects.findHouseholdForUpdate(message.householdId());
        if (matches.size() != 1) {
            log.warn("대상자가 등록되지 않은 가구의 이벤트 건너뜀: householdId={}, eventId={}, eventType={}",
                    message.householdId(), message.eventId(), message.eventType());
            return;
        }

        // 재전송이나 파티션 재배치로 같은 이벤트가 다시 와도 알림을 다시 만들지 않는다.
        // 분석 서비스가 결정적 event_id를 쓰므로 이 검사만으로 이벤트당 알림 1건이 보장된다.
        if (events.existsById(message.eventId())) {
            return;
        }

        Subject subject = matches.get(0);
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

        // 예약된 외출의 시작·종료를 이 자리에서 반영한다.
        // 스케줄러가 늦게 돌아도 아래 억제 판정이 어긋나지 않게 한다.
        subject.syncMonitoring(now);

        EventRoutingPolicy.Route route = routing.routeOf(message.eventType());
        RiskLevel level = route.mode() == EventRoutingPolicy.Mode.IMMEDIATE
                ? route.level()
                : RiskLevel.NORMAL;

        // 계약에 score가 없다. 등급의 대표값을 저장 점수로 삼아
        // 이상 징후 목록과 위험 추이가 기존과 같은 열을 읽을 수 있게 한다.
        AnalysisEvent stored = events.saveAndFlush(new AnalysisEvent(
                message.eventId(),
                subject.getId(),
                message.householdId(),
                message.eventType(),
                message.applianceType(),
                riskProperties.representativeScore(level),
                level,
                message.occurredAt(),
                reasonJson(message),
                subject.getRiskPolicyId()
        ));

        if (route.mode() == EventRoutingPolicy.Mode.EVIDENCE) {
            // 저장만 한다. 보고서 근거로 남기고 화면 갱신도 일으키지 않는다.
            log.debug("근거로만 저장한 이벤트: eventId={}, eventType={}",
                    message.eventId(), message.eventType());
            return;
        }

        // SHADOW와 IMMEDIATE는 화면에 보인다. 커밋 후 한 번만 내보낸다.
        publisher.publishEvent(
                new SubjectStateChanged(subject.getId(), StateChangeTrigger.DETECTION));

        if (route.mode() == EventRoutingPolicy.Mode.SHADOW) {
            // 등급도 알림도 없이 자체 평가와 나란히 두고 비교하는 기간이다.
            log.debug("그림자 모드로 저장한 이벤트: eventId={}, eventType={}",
                    message.eventId(), message.eventType());
            return;
        }

        applyImmediate(subject, stored, level, message, now);
    }

    /** 즉시 알림 경로. 등급 슬롯을 세우고 알림을 만든다. */
    private void applyImmediate(
            Subject subject,
            AnalysisEvent stored,
            RiskLevel level,
            AnalysisEventMessage message,
            OffsetDateTime now
    ) {
        subject.applyEventRisk(
                level,
                riskProperties.representativeScore(level),
                message.eventId(),
                applianceOf(message),
                now
        );

        // 사건이 일어난 시각에 외출 중이었는지로 판정한다.
        // 이벤트가 늦게 도착해도 같은 답이 나와야 한다.
        boolean occurredWhileAway = subject.isAwayAt(message.occurredAt());
        if (occurredWhileAway && routing.suppressWhileAway(message.eventType())) {
            log.debug("외출 중 발생이라 알림을 억제한 이벤트: eventId={}, eventType={}",
                    message.eventId(), message.eventType());
            return;
        }

        // 기기가 꺼져 있던 구간의 무활동은 사람에 대한 신호가 아니다.
        // 구분하지 않으면 Wi-Fi가 끊길 때마다 보호자에게 위험 알림이 간다.
        // 외출과 같은 기준으로 "사건이 일어난 시각"에 끊겨 있었는지를 본다.
        if (routing.suppressWhileDeviceOffline(message.eventType())
                && deviceConnectivity.wasDisconnectedAt(subject.getHouseholdId(), message.occurredAt())) {
            log.info("기기 오프라인 구간의 이벤트라 알림을 억제: eventId={}, eventType={}, householdId={}",
                    message.eventId(), message.eventType(), subject.getHouseholdId());
            return;
        }
        if (subject.getAuthSub() == null || subject.getAuthSub().isBlank()) {
            log.warn("이벤트 저장 완료, 수신자 식별자 없음: subjectId={}", subject.getId());
            return;
        }

        // 알림은 모두 "현재 위험하신가요?"에 예/아니오 응답을 받는다. 유형별로 나누지 않는다.
        Notification notification = notifications.createNotification(
                message.eventId(),
                null,
                subject.getId(),
                subject.getAuthSub(),
                true,
                routing.responseDeadline()
        );
        subject.markAlerted(now);

        // 담당자가 이 등급의 알림을 꺼 두었으면 행은 남기되 발송하지 않는다.
        if (!notificationGate.allows(subject, level, NotificationSetting.Channel.PUSH)) {
            log.info("담당자 수신 설정이 꺼져 있어 발송하지 않는다: subjectId={}, level={}",
                    subject.getId(), level);
            return;
        }
        if (pushEnabled) {
            log.info("이벤트 알림 생성, 웹푸시 발송 요청: notificationId={}, subjectId={}, eventId={}, "
                            + "eventType={}, level={}",
                    notification.getId(), subject.getId(), message.eventId(),
                    message.eventType(), level);
            // 응답 화면의 "예"는 위험(도움 요청)이다. 질문도 그 방향으로 묻는다.
            String body = narrator.describeDetail(stored);
            publisher.publishEvent(new NotificationReady(
                    notification.getId(),
                    "안전 확인 요청",
                    body + ". " + NotificationReady.RESPONSE_QUESTION
            ));
        } else {
            // 알림 행은 남았는데 폰에 아무것도 안 오는 상황의 이유를 그대로 남긴다.
            log.info("웹푸시가 꺼져 있어 발송하지 않는다: notificationId={}, subjectId={}",
                    notification.getId(), subject.getId());
        }
    }

    /**
     * reason의 {@code appliance_type}을 이벤트 등급 슬롯의 해제 기준으로 삼는다.
     * 메시지 본문에 가전이 따로 실려 오면 그 값을 먼저 쓴다.
     */
    private String applianceOf(AnalysisEventMessage message) {
        if (message.applianceType() != null && !message.applianceType().isBlank()) {
            return message.applianceType();
        }
        Object fromReason = message.reason().get("appliance_type");
        return fromReason instanceof String appliance && !appliance.isBlank() ? appliance : null;
    }

    private String reasonJson(AnalysisEventMessage message) {
        try {
            return objectMapper.writeValueAsString(message.reason());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("이벤트 사유를 저장할 수 없습니다.", e);
        }
    }

    /**
     * 반영 전에 꼭 있어야 하는 값들.
     *
     * <p>{@code score}는 검사하지 않는다. 분석 서비스의 이벤트 계약에 없는 필드라
     * 필수로 두면 실제 이벤트가 도착하는 순간 컨슈머 컨테이너가 멈춘다.
     * 값이 실려 오더라도 무시하고, 저장 점수는 라우팅 등급의 대표값으로 채운다.
     */
    private void validate(AnalysisEventMessage message) {
        if (message == null || message.eventId() == null || message.householdId() == null
                || message.householdId().isBlank()
                || message.occurredAt() == null || message.reason() == null) {
            throw new IllegalArgumentException("분석 이벤트 필수 필드가 유효하지 않습니다.");
        }
    }
}

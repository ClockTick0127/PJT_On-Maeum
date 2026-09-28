package com.nilm.monitoring.service;

import com.nilm.monitoring.config.enums.StateChangeTrigger;
import com.nilm.monitoring.domain.AnalysisEvent;
import com.nilm.monitoring.domain.Manager;
import com.nilm.monitoring.domain.Notification;
import com.nilm.monitoring.domain.Subject;
import com.nilm.monitoring.dto.SubjectStatusEvent;
import com.nilm.monitoring.repository.AnalysisEventRepository;
import com.nilm.monitoring.repository.ManagerRepository;
import com.nilm.monitoring.repository.NotificationRepository;
import com.nilm.monitoring.repository.SubjectRepository;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * 담당자 대시보드 실시간 스트림.
 * 담당자는 토큰 주인으로만 식별하므로 남의 대상자 스트림을 여는 경로 자체가 없다.
 */
@Service
@RequiredArgsConstructor
public class SubjectStatusStreamService {

    /** 대상자 1명의 상태가 바뀌었음을 알리는 SSE 이벤트 이름. */
    static final String SUBJECT_STATUS_EVENT = "subject-status";

    /** 브라우저가 열어 둔 연결은 서버가 먼저 끊지 않는다(0 = 무제한). */
    private static final long STREAM_TIMEOUT_MILLIS = 0L;

    /** "이상 건수"로 세는 기간. 대시보드 카드가 보여주는 최근 하루치. */
    private static final Duration RECENT_EVENT_WINDOW = Duration.ofHours(24);

    private final SubjectStatusStreamRegistry registry;
    private final ManagerRepository managers;
    private final SubjectRepository subjects;
    private final AnalysisEventRepository events;
    private final NotificationRepository notifications;
    private final AnalysisEventNarrator narrator;

    /** 로그인한 담당자의 연결을 연다. 담당자로 등록되지 않은 토큰은 열 수 없다. */
    @Transactional(readOnly = true)
    public SseEmitter subscribe(String managerAuthSub) {
        if (managerAuthSub == null || managerAuthSub.isBlank()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "로그인이 필요합니다.");
        }

        Manager manager = managers.findByAuthSub(managerAuthSub)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.FORBIDDEN,
                        "등록된 담당자만 실시간 상태를 구독할 수 있습니다."
                ));

        return registry.register(manager.getId(), STREAM_TIMEOUT_MILLIS);
    }

    /**
     * 바뀐 대상자 1명의 현재 상태를 배정 담당자에게 보낸다.
     *
     * <p>커밋 이후에 호출되지만 커밋한 트랜잭션의 영속성 컨텍스트가 아직 스레드에 남아 있어,
     * 벌크 갱신({@code touchState})이 반영된 값을 보려면 새 트랜잭션에서 다시 읽어야 한다.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public void publish(Long subjectId, StateChangeTrigger trigger) {
        subjects.findById(subjectId).ifPresent(subject -> {
            Long managerId = subject.getManagerId();
            if (managerId == null || !registry.hasSubscriber(managerId)) {
                return;
            }
            registry.send(managerId, SUBJECT_STATUS_EVENT, toEvent(subject, trigger));
        });
    }

    private SubjectStatusEvent toEvent(Subject subject, StateChangeTrigger trigger) {
        OffsetDateTime recentFrom = OffsetDateTime.now(ZoneOffset.UTC).minus(RECENT_EVENT_WINDOW);

        return new SubjectStatusEvent(
                subject.getId().toString(),
                subject.getStateVersion(),
                trigger,
                subject.getCurrentRiskLevel(),
                subject.getCurrentRiskScore(),
                subject.getAssessmentStatus(),
                subject.getAssessmentConfidence(),
                subject.riskSource(),
                events.countBySubjectIdAndOccurredAtGreaterThanEqual(subject.getId(), recentFrom),
                toLastActivity(subject),
                notifications.countUnresolvedBySubjectId(subject.getId()),
                notifications.findLatestAlertBySubjectId(subject.getId())
                        .map(this::toLatestAlert)
                        .orElse(null),
                events.findLatestBySubjectId(subject.getId())
                        .map(this::toLastDetection)
                        .orElse(null),
                subject.getUpdatedAt()
        );
    }

    private SubjectStatusEvent.LastActivity toLastActivity(Subject subject) {
        if (subject.getLastActivityAt() == null) {
            return null;
        }
        return new SubjectStatusEvent.LastActivity(
                subject.getLastActivityAt(),
                subject.getLastActivityAppliance()
        );
    }

    private SubjectStatusEvent.LatestAlert toLatestAlert(Notification alert) {
        String answer = alert.getUserResponse() == null
                ? null
                : alert.getUserResponse() ? "YES" : "NO";
        return new SubjectStatusEvent.LatestAlert(
                alert.getId().toString(),
                alert.getEventId() == null ? null : alert.getEventId().toString(),
                alert.getManagerResponseStatus(),
                alert.getManagerStatusUpdatedAt(),
                new SubjectStatusEvent.SubjectResponse(
                        alert.getResponseStatus(),
                        answer,
                        alert.getRespondedAt()
                )
        );
    }

    private SubjectStatusEvent.LastDetection toLastDetection(AnalysisEvent event) {
        return new SubjectStatusEvent.LastDetection(
                event.getId().toString(),
                event.getEventType(),
                event.getApplianceType(),
                narrator.describe(event),
                event.getOccurredAt(),
                narrator.parseReason(event)
        );
    }
}

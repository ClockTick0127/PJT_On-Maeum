package com.nilm.monitoring.service;

import com.nilm.monitoring.config.enums.StateChangeTrigger;
import com.nilm.monitoring.domain.Notification;
import com.nilm.monitoring.domain.Subject;
import com.nilm.monitoring.dto.NotificationResponseDto;
import com.nilm.monitoring.dto.NotificationResponseRequest;
import com.nilm.monitoring.repository.NotificationRepository;
import com.nilm.monitoring.repository.AnalysisEventRepository;
import com.nilm.monitoring.repository.SubjectRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class NotificationService {

    /** 인증 연결 전의 테스트값. 운영 기한은 호출하는 쪽이 정책에서 읽어 넘긴다. */
    private static final Duration DEFAULT_RESPONSE_WINDOW = Duration.ofSeconds(30);

    private final NotificationRepository repository;
    private final AnalysisEventRepository events;
    private final SubjectRepository subjects;
    private final SubjectAccessGuard accessGuard;
    private final ApplicationEventPublisher publisher;

    @Value("${app.push.test-auth-sub}")
    private String testAuthSub;

    @Transactional
    public NotificationResponseDto respond(
            Long notificationId,
            String authSub,
            NotificationResponseRequest request
    ) {
        if (authSub == null || authSub.isBlank()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "로그인이 필요합니다.");
        }

        Notification notification = repository
                .findForResponse(notificationId)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        "알림을 찾을 수 없습니다."
                ));

        // 토큰 주인에게 온 알림만 응답할 수 있다.
        if (!authSub.equals(notification.getAuthSub())) {
            throw new ResponseStatusException(
                    HttpStatus.FORBIDDEN,
                    "본인에게 온 알림만 응답할 수 있습니다."
            );
        }

        boolean answer = request.answer().equals("yes");

        boolean newlyAnswered = notification.getResponseStatus()
                != Notification.ResponseStatus.ANSWERED;
        if (!newlyAnswered) {

            // 동일 응답 재전송은 성공, 다른 응답으로 변경은 거부
            if (!Boolean.valueOf(answer)
                    .equals(notification.getUserResponse())) {
                throw new ResponseStatusException(
                        HttpStatus.CONFLICT,
                        "이미 다른 응답이 저장되었습니다."
                );
            }
        } else {
            try {
                notification.answer(
                        answer,
                        OffsetDateTime.now(ZoneOffset.UTC)
                );
            } catch (IllegalStateException e) {
                throw new ResponseStatusException(
                        HttpStatus.CONFLICT,
                        e.getMessage()
                );
            }
        }

        if (newlyAnswered) {
            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
            touchSubject(notification, now, StateChangeTrigger.SUBJECT_RESPONSE);
        }

        return new NotificationResponseDto(
                notification.getId().toString(),
                Boolean.TRUE.equals(notification.getUserResponse())
                        ? "yes" : "no",
                request.respondedAt(),
                request.source()
        );
    }

    /**
     * 담당자가 알림 처리 상태를 바꾼다.
     *
     * <p>조치 완료로 바뀌면 이벤트 등급 슬롯을 해제한다(설계 11.4절의 해제 조건 중 하나).
     * 담당자가 이미 확인하고 끝낸 사건이 계속 유효 등급을 붙들고 있으면,
     * 그 뒤의 자체 평가 결과가 화면에 드러나지 않는다.
     */
    @Transactional
    public void changeManagerStatus(
            Long notificationId,
            String managerAuthSub,
            Notification.ManagerResponseStatus status
    ) {
        if (managerAuthSub == null || managerAuthSub.isBlank()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "로그인이 필요합니다.");
        }

        Notification notification = repository
                .findForResponse(notificationId)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        "알림을 찾을 수 없습니다."
                ));

        Long subjectId = subjectOf(notification).orElseThrow(() -> new ResponseStatusException(
                HttpStatus.NOT_FOUND,
                "알림에 연결된 대상자를 찾을 수 없습니다."
        ));
        Subject subject = accessGuard.requireManager(managerAuthSub, subjectId);

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        notification.changeManagerStatus(status, now);

        if (status == Notification.ManagerResponseStatus.RESOLVED
                && notification.getEventId() != null
                && notification.getEventId().equals(subject.getEventRiskEventId())) {
            subject.clearEventRisk(now);
        } else {
            subject.touch(now);
        }
        publisher.publishEvent(
                new SubjectStateChanged(subjectId, StateChangeTrigger.MANAGER_STATUS));
    }

    @Transactional
    public void expireOverdue() {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        var overdue = repository.findAllByResponseStatusAndResponseDeadlineLessThanEqual(
                Notification.ResponseStatus.PENDING,
                now
        );
        for (Notification notification : overdue) {
            notification.expireIfOverdue(now);
            touchSubject(notification, now, StateChangeTrigger.RESPONSE_EXPIRED);
        }
    }

    // 인증 연결 전, 구독 등록과 동일한 테스트 사용자에게 알림 생성
    @Transactional
    public Notification createNotification(UUID eventId, boolean responseRequired) {
        return createNotification(eventId, testAuthSub, responseRequired);
    }

    @Transactional
    public Notification createNotification(
            UUID eventId,
            String recipientAuthSub,
            boolean responseRequired
    ) {
        return createNotification(
                eventId, null, null, recipientAuthSub, responseRequired, DEFAULT_RESPONSE_WINDOW);
    }

    /**
     * 알림 한 건을 만든다.
     *
     * @param eventId 분석 서비스 이벤트에서 나온 알림이면 그 이벤트
     * @param assessmentId 모니터링 자체 평가에서 나온 알림이면 그 평가
     * @param subjectId 누구에 대한 알림인지. 자체 평가 알림은 이벤트로 거슬러 찾을 수 없다
     * @param responseWindow 응답 기한까지의 길이
     */
    @Transactional
    public Notification createNotification(
            UUID eventId,
            UUID assessmentId,
            Long subjectId,
            String recipientAuthSub,
            boolean responseRequired,
            Duration responseWindow
    ) {
        Notification notification = new Notification(
                eventId,
                assessmentId,
                subjectId,
                recipientAuthSub
        );

        if (responseRequired) {
            notification.requestResponse(
                    OffsetDateTime.now(ZoneOffset.UTC).plus(responseWindow)
            );
        }

        return repository.save(notification);
    }

    /**
     * 발송 결과 저장
     * */
    @Transactional
    public void updateSendResult(Long notificationId, boolean success) {
        Notification notification = repository.findForResponse(notificationId)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        "알림을 찾을 수 없습니다."
                ));

        if (success) {
            notification.markSent();
        } else if (notification.getSendStatus()
                != Notification.SendStatus.SENT) {
            notification.markFailed();
        }
    }

    /**
     * 알림이 가리키는 대상자의 상태 버전을 올린다.
     *
     * <p>알림 행의 subject_id를 먼저 본다. 자체 평가가 만든 알림은 분석 이벤트에
     * 걸리지 않아 event_id로는 대상자를 찾을 수 없기 때문이다.
     * subject_id가 비어 있는 과거 행만 이벤트를 거쳐 찾는다.
     */
    private void touchSubject(
            Notification notification,
            OffsetDateTime now,
            StateChangeTrigger trigger
    ) {
        subjectOf(notification).ifPresent(subjectId -> {
            subjects.touchState(subjectId, now);
            // 커밋된 뒤에 담당자 대시보드로 흘려보낸다.
            publisher.publishEvent(new SubjectStateChanged(subjectId, trigger));
        });
    }

    private Optional<Long> subjectOf(Notification notification) {
        if (notification.getSubjectId() != null) {
            return Optional.of(notification.getSubjectId());
        }
        if (notification.getEventId() == null) {
            return Optional.empty();
        }
        return events.findById(notification.getEventId())
                .map(event -> event.getSubjectId());
    }

}

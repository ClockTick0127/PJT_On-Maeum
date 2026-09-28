package com.nilm.monitoring.domain;

import jakarta.persistence.*;
import lombok.Getter;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * 알림 발송 Entity
 * 알림 응답과 상태, 기한 관리
 * */
@Getter
@Entity
@Table(name = "notifications")
public class Notification {

    public enum SendStatus {
        PENDING,
        SENT,
        FAILED
    }

    /** 담당자가 이 알림을 어디까지 처리했는지. */
    public enum ManagerResponseStatus {
        UNCONFIRMED, // 미확인
        ACKNOWLEDGED, // 확인
        RESOLVED // 조치 완료
    }

    public enum ResponseStatus {
        NOT_REQUIRED, // 응답 불필요 일일 알림
        PENDING, // 응답 대기
        ANSWERED, // 예 또는 아니오 응답 완료
        EXPIRED // 미응답 상태로 기한 만료
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // 일일 요약 알림은 이벤트 없이 생성 가능
    @Column(name = "event_id")
    private UUID eventId;

    /**
     * 모니터링 자체 평가가 만든 알림이 가리키는 평가 이력.
     * 분석 이벤트에서 나온 알림이면 비어 있다.
     */
    @Column(name = "assessment_id")
    private UUID assessmentId;

    /**
     * 이 알림이 누구에 대한 것인지.
     * 자체 평가 알림은 분석 이벤트에 걸리지 않아 event_id로 대상자를 찾을 수 없다.
     */
    @Column(name = "subject_id")
    private Long subjectId;

    @Column(name = "auth_sub", nullable = false)
    private String authSub;

    @Column(name = "response_deadline")
    private OffsetDateTime responseDeadline; // 응답 기한

    @Column(name = "user_response")
    private Boolean userResponse; // 예: true 아니오: false 미응답: null

    @Column(name = "responded_at")
    private OffsetDateTime respondedAt;

    @Enumerated(EnumType.STRING)
    @Column(name = "send_status", nullable = false)
    private SendStatus sendStatus = SendStatus.PENDING; // 발송 상태

    @Enumerated(EnumType.STRING)
    @Column(name = "response_status", nullable = false)
    private ResponseStatus responseStatus = ResponseStatus.NOT_REQUIRED; // 응답 상태

    @Enumerated(EnumType.STRING)
    @Column(name = "manager_response_status", nullable = false)
    private ManagerResponseStatus managerResponseStatus =
            ManagerResponseStatus.UNCONFIRMED; // 담당자 처리 상태

    @Column(name = "manager_status_updated_at")
    private OffsetDateTime managerStatusUpdatedAt;

    /** 알림 행이 만들어진 시각. 발송 지연을 재는 기준점이다. */
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    /** 상태가 마지막으로 바뀐 시각. 상태 변경 메서드가 갱신한다. */
    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    protected Notification() {
    }

    public Notification(UUID eventId, String authSub) {
        this(eventId, null, null, authSub);
    }

    public Notification(UUID eventId, UUID assessmentId, Long subjectId, String authSub) {
        if (authSub == null || authSub.isBlank()) {
            throw new IllegalArgumentException("알림 수신자 ID가 필요합니다.");
        }

        this.eventId = eventId;
        this.assessmentId = assessmentId;
        this.subjectId = subjectId;
        this.authSub = authSub;

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        this.createdAt = now;
        this.updatedAt = now;
    }

    public void requestResponse(OffsetDateTime deadline) {
        if (responseStatus != ResponseStatus.NOT_REQUIRED) {
            throw new IllegalStateException("이미 응답 요청이 설정되었습니다.");
        }
        if (deadline == null) {
            throw new IllegalArgumentException("응답 기한이 필요합니다.");
        }

        this.responseDeadline = deadline;
        this.responseStatus = ResponseStatus.PENDING;
        touch(OffsetDateTime.now(ZoneOffset.UTC));
    }

    public void answer(boolean answer, OffsetDateTime now) {
        if (responseStatus != ResponseStatus.PENDING) {
            throw new IllegalStateException("응답 대기 중인 알림이 아닙니다.");
        }
        if (!now.isBefore(responseDeadline)) {
            throw new IllegalStateException("응답 기한이 지났습니다.");
        }

        this.userResponse = answer;
        this.respondedAt = now;
        this.responseStatus = ResponseStatus.ANSWERED;
        touch(now);
    }

    public void expireIfOverdue(OffsetDateTime now) {
        if (responseStatus == ResponseStatus.PENDING
                && !now.isBefore(responseDeadline)) {
            this.responseStatus = ResponseStatus.EXPIRED;
            touch(now);
        }
    }

    public void changeManagerStatus(
            ManagerResponseStatus status,
            OffsetDateTime now
    ) {
        if (status == null) {
            throw new IllegalArgumentException("담당자 처리 상태가 필요합니다.");
        }
        if (this.managerResponseStatus == status) {
            return;
        }

        this.managerResponseStatus = status;
        this.managerStatusUpdatedAt = now;
        touch(now);
    }

    public void markSent() {
        this.sendStatus = SendStatus.SENT;
        touch(OffsetDateTime.now(ZoneOffset.UTC));
    }

    public void markFailed() {
        if (sendStatus == SendStatus.SENT) {
            throw new IllegalStateException("발송 완료된 알림입니다.");
        }
        this.sendStatus = SendStatus.FAILED;
        touch(OffsetDateTime.now(ZoneOffset.UTC));
    }

    /** 상태 변경 시각을 기록한다. 시계가 뒤로 가도 갱신 시각은 뒤로 가지 않는다. */
    private void touch(OffsetDateTime now) {
        if (now == null) {
            now = OffsetDateTime.now(ZoneOffset.UTC);
        }
        if (this.updatedAt == null || now.isAfter(this.updatedAt)) {
            this.updatedAt = now;
        }
    }
}

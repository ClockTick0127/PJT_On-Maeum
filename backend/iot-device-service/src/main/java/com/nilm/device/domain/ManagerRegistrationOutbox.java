package com.nilm.device.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * 담당자 가입 이벤트의 내구 저장소.
 *
 * <p>가입 트랜잭션에서 함께 쓰이고, 릴레이가 Kafka로 발행한 뒤 PUBLISHED로 옮긴다.
 * 발행 직후 죽으면 같은 이벤트가 두 번 나갈 수 있으므로 소비 쪽이 멱등해야 한다.
 */
@Entity
@Table(name = "manager_registration_outbox")
public class ManagerRegistrationOutbox {

    /** 오류 메시지는 진단용이라 컬럼을 채우지 않을 만큼만 남긴다. */
    private static final int ERROR_LIMIT = 2000;

    public enum Status {
        PENDING,
        PUBLISHED
    }

    @Id
    @Column(name = "event_id")
    private UUID eventId;

    @Column(name = "keycloak_user_id", nullable = false, unique = true)
    private UUID keycloakUserId;

    @Column(nullable = false, length = 100)
    private String email;

    @Column(name = "display_name", nullable = false, length = 50)
    private String displayName;

    @Column(length = 20)
    private String phone;

    @Column(nullable = false, length = 100)
    private String organization;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private Status status = Status.PENDING;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    @Column(name = "next_attempt_at", nullable = false)
    private OffsetDateTime nextAttemptAt;

    @Column(name = "last_error", columnDefinition = "text")
    private String lastError;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "published_at")
    private OffsetDateTime publishedAt;

    protected ManagerRegistrationOutbox() {
    }

    public ManagerRegistrationOutbox(UserProfile profile, OffsetDateTime now) {
        // 사람당 한 행이므로 사용자 ID를 그대로 이벤트 ID로 쓴다.
        // 같은 가입이 두 번 적히면 유일 제약에 걸려 드러난다.
        this.eventId = profile.getKeycloakUserId();
        this.keycloakUserId = profile.getKeycloakUserId();
        this.email = profile.getEmail();
        this.displayName = profile.getDisplayName();
        this.phone = profile.getPhone();
        this.organization = profile.getOrganization();
        this.status = Status.PENDING;
        this.attemptCount = 0;
        this.nextAttemptAt = now;
        this.createdAt = now;
    }

    public void markPublished(OffsetDateTime now) {
        this.status = Status.PUBLISHED;
        this.attemptCount += 1;
        this.publishedAt = now;
        this.lastError = null;
    }

    public void markFailed(OffsetDateTime now, Duration retryAfter, String error) {
        this.attemptCount += 1;
        this.nextAttemptAt = now.plus(retryAfter);
        this.lastError = error == null || error.length() <= ERROR_LIMIT
                ? error
                : error.substring(0, ERROR_LIMIT);
    }

    public UUID getEventId() {
        return eventId;
    }

    public UUID getKeycloakUserId() {
        return keycloakUserId;
    }

    public String getEmail() {
        return email;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getPhone() {
        return phone;
    }

    public String getOrganization() {
        return organization;
    }

    public Status getStatus() {
        return status;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public OffsetDateTime getNextAttemptAt() {
        return nextAttemptAt;
    }

    public String getLastError() {
        return lastError;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getPublishedAt() {
        return publishedAt;
    }
}

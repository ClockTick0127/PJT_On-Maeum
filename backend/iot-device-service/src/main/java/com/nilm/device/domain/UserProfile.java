package com.nilm.device.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * 서비스 프로필 — Keycloak 사용자 1명당 1행.
 * 비밀번호·세션은 Keycloak 소관이며 여기에는 저장하지 않는다.
 */
@Entity
@Table(name = "user_profiles")
public class UserProfile {

    public enum Status {
        /** 승인 절차를 두지 않기로 해 더는 부여하지 않는다. 과거 행을 읽기 위해 남긴다. */
        PENDING,
        ACTIVE,
        SUSPENDED
    }

    @Id
    @Column(name = "keycloak_user_id")
    private UUID keycloakUserId;

    @Column(nullable = false, unique = true, length = 100)
    private String email;

    @Column(name = "display_name", nullable = false, length = 50)
    private String displayName;

    @Column(length = 20)
    private String phone;

    /** 기관 소속(복지사)이면 값이 있다. 이 가입만 monitoring의 담당자 명단으로 흘러간다. */
    @Column(length = 100)
    private String organization;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 15)
    private Status status = Status.ACTIVE;

    /**
     * 초기 비밀번호를 아직 쓰고 있다는 표시.
     * 담당자가 대리 생성하면 켜지고, 본인이 비밀번호를 바꾸면 꺼진다.
     * 켜져 있는 동안의 응답은 담당자가 대신 눌렀을 가능성이 있다.
     */
    @Column(name = "password_reset_required", nullable = false)
    private boolean passwordResetRequired = false;

    @Column(name = "created_at", nullable = false, updatable = false, insertable = false)
    private OffsetDateTime createdAt;

    protected UserProfile() {
    }

    public UserProfile(UUID keycloakUserId, String email, String displayName,
                       String phone, String organization) {
        this(keycloakUserId, email, displayName, phone, organization, false);
    }

    public UserProfile(UUID keycloakUserId, String email, String displayName,
                       String phone, String organization, boolean passwordResetRequired) {
        this.keycloakUserId = keycloakUserId;
        this.email = email;
        this.displayName = displayName;
        this.phone = phone;
        this.organization = organization;
        // 승인 절차가 없으므로 기관 소속 가입도 바로 활성이다.
        this.status = Status.ACTIVE;
        this.passwordResetRequired = passwordResetRequired;
    }

    /** 본인이 비밀번호를 바꾸면 초기 비밀번호 표시를 내린다. */
    public void clearPasswordResetRequired() {
        this.passwordResetRequired = false;
    }

    public boolean isPasswordResetRequired() {
        return passwordResetRequired;
    }

    /**
     * 프로필 부분 수정 — null은 "바꾸지 않음"이다.
     *
     * <p>이메일은 Keycloak의 사용자명이자 로그인 키라 여기서 바꾸지 않는다.
     * 기관 소속도 제외한다 — 바뀌면 monitoring의 담당자 명단까지 따라가야 하므로
     * 별도 절차가 필요하다.
     */
    public void updateProfile(String displayName, String phone) {
        if (displayName != null && !displayName.isBlank()) {
            this.displayName = displayName;
        }
        if (phone != null) {
            this.phone = phone.isBlank() ? null : phone;
        }
    }

    public void suspend() {
        this.status = Status.SUSPENDED;
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

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
}

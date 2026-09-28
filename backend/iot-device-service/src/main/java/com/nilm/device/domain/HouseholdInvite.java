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
 * 가구 초대 코드 — 로그인 수단이 아니라 가구 접근 신청권.
 * 코드만으로는 아무것도 할 수 없고, 신원이 있는 계정이 이 코드를 사용해야
 * 가구 멤버십이 생긴다. 1회용이며 만료·회수가 가능하고 사용 이력이 남는다.
 */
@Entity
@Table(name = "household_invites")
public class HouseholdInvite {

    @Id
    @Column(length = 12)
    private String code;

    @Column(name = "house_id", nullable = false, length = 10)
    private String houseId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 10)
    private HouseholdMember.Relation relation;

    @Column(name = "created_by", nullable = false)
    private UUID createdBy;

    @Column(name = "created_at", nullable = false, updatable = false, insertable = false)
    private OffsetDateTime createdAt;

    @Column(name = "expires_at", nullable = false)
    private OffsetDateTime expiresAt;

    @Column(name = "used_at")
    private OffsetDateTime usedAt;

    @Column(name = "used_by")
    private UUID usedBy;

    @Column(name = "revoked_at")
    private OffsetDateTime revokedAt;

    protected HouseholdInvite() {
    }

    public HouseholdInvite(String code, String houseId, HouseholdMember.Relation relation,
                           UUID createdBy, OffsetDateTime expiresAt) {
        this.code = code;
        this.houseId = houseId;
        this.relation = relation;
        this.createdBy = createdBy;
        this.expiresAt = expiresAt;
    }

    /** 사용 불가 사유 — 사용 가능하면 null */
    public String unusableReason(OffsetDateTime now) {
        if (revokedAt != null) {
            return "회수된 초대 코드입니다";
        }
        if (usedAt != null) {
            return "이미 사용된 초대 코드입니다";
        }
        if (expiresAt.isBefore(now)) {
            return "만료된 초대 코드입니다";
        }
        return null;
    }

    public void use(UUID userId, OffsetDateTime now) {
        this.usedBy = userId;
        this.usedAt = now;
    }

    public void revoke(OffsetDateTime now) {
        this.revokedAt = now;
    }

    public String getCode() {
        return code;
    }

    public String getHouseId() {
        return houseId;
    }

    public HouseholdMember.Relation getRelation() {
        return relation;
    }

    public UUID getCreatedBy() {
        return createdBy;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getExpiresAt() {
        return expiresAt;
    }

    public OffsetDateTime getUsedAt() {
        return usedAt;
    }

    public UUID getUsedBy() {
        return usedBy;
    }

    public OffsetDateTime getRevokedAt() {
        return revokedAt;
    }
}

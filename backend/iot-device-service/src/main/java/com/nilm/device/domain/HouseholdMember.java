package com.nilm.device.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;

/**
 * 가구 멤버십 — 누가 어느 가구에 어떤 관계로 연결됐는지.
 * 계정(Keycloak)과 가구를 잇는 유일한 연결점이며, 한 사람이 여러 가구에
 * 서로 다른 관계로 속할 수 있다. (예: 내 가구는 SELF, 담당 가구는 STAFF)
 */
@Entity
@Table(name = "household_members")
@IdClass(HouseholdMember.MemberId.class)
public class HouseholdMember {

    /**
     * 가족·지인 보호자(GUARDIAN)는 제거했다. 멤버십은 만들 수 있었지만 monitoring 쪽에
     * 대응 개념이 없어 어떤 데이터도 볼 수 없었고(전 조회 403), 절반만 구현된 역할을
     * 남겨두면 사용자에게 고장으로 보인다. 도입하려면 두 서비스의 권한 모델을
     * 함께 설계해야 한다 — docs/권한모델_소유권_결정요청.md 참고.
     */
    public enum Relation {
        /** 대상자 본인 — 1인 가구 자가 모니터링 */
        SELF,
        /** 복지사 등 기관 담당자 */
        STAFF
    }

    /** 알림 발송 순서 */
    public enum NotifyPriority {
        PRIMARY, SECONDARY
    }

    @Id
    @Column(name = "house_id", length = 10)
    private String houseId;

    @Id
    @Column(name = "keycloak_user_id")
    private UUID keycloakUserId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 10)
    private Relation relation;

    @Enumerated(EnumType.STRING)
    @Column(name = "notify_priority", nullable = false, length = 10)
    private NotifyPriority notifyPriority = NotifyPriority.PRIMARY;

    @Column(name = "notify_phone", length = 20)
    private String notifyPhone;

    @Column(name = "notify_enabled", nullable = false)
    private boolean notifyEnabled = true;

    @Column(name = "joined_at", nullable = false, updatable = false, insertable = false)
    private OffsetDateTime joinedAt;

    protected HouseholdMember() {
    }

    public HouseholdMember(String houseId, UUID keycloakUserId, Relation relation,
                           NotifyPriority notifyPriority, String notifyPhone, Boolean notifyEnabled) {
        this.houseId = houseId;
        this.keycloakUserId = keycloakUserId;
        // SELF(본인)와 STAFF(기관 담당자) 사이에는 합리적인 기본값이 없다.
        // 조용히 한쪽으로 정해지면 권한이 잘못 부여되므로 명시를 요구한다.
        if (relation == null) {
            throw new IllegalArgumentException("가구와의 관계(relation)는 필수입니다");
        }
        this.relation = relation;
        if (notifyPriority != null) {
            this.notifyPriority = notifyPriority;
        }
        this.notifyPhone = notifyPhone;
        if (notifyEnabled != null) {
            this.notifyEnabled = notifyEnabled;
        }
    }

    /** 부분 수정 — null인 필드는 유지 */
    public void update(Relation relation, NotifyPriority notifyPriority,
                       String notifyPhone, Boolean notifyEnabled) {
        if (relation != null) {
            this.relation = relation;
        }
        if (notifyPriority != null) {
            this.notifyPriority = notifyPriority;
        }
        if (notifyPhone != null) {
            this.notifyPhone = notifyPhone;
        }
        if (notifyEnabled != null) {
            this.notifyEnabled = notifyEnabled;
        }
    }

    public String getHouseId() {
        return houseId;
    }

    public UUID getKeycloakUserId() {
        return keycloakUserId;
    }

    public Relation getRelation() {
        return relation;
    }

    public NotifyPriority getNotifyPriority() {
        return notifyPriority;
    }

    public String getNotifyPhone() {
        return notifyPhone;
    }

    public boolean isNotifyEnabled() {
        return notifyEnabled;
    }

    public OffsetDateTime getJoinedAt() {
        return joinedAt;
    }

    /** 복합 키 (house_id + keycloak_user_id) */
    public static class MemberId implements Serializable {

        private String houseId;
        private UUID keycloakUserId;

        public MemberId() {
        }

        public MemberId(String houseId, UUID keycloakUserId) {
            this.houseId = houseId;
            this.keycloakUserId = keycloakUserId;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof MemberId other
                    && Objects.equals(houseId, other.houseId)
                    && Objects.equals(keycloakUserId, other.keycloakUserId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(houseId, keycloakUserId);
        }
    }
}

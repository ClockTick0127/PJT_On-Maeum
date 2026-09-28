package com.nilm.device.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class HouseholdInviteTest {

    private static final UUID CREATOR = UUID.randomUUID();
    private static final UUID ACCEPTOR = UUID.randomUUID();
    private static final OffsetDateTime NOW = OffsetDateTime.of(
            2026, 9, 17, 12, 0, 0, 0, ZoneOffset.UTC);

    private HouseholdInvite invite(OffsetDateTime expiresAt) {
        return new HouseholdInvite("ABCD2345", "H001",
                HouseholdMember.Relation.STAFF, CREATOR, expiresAt);
    }

    @Test
    @DisplayName("만료 전이고 사용·회수되지 않았으면 사용할 수 있다")
    void usableWhenFresh() {
        assertNull(invite(NOW.plusHours(72)).unusableReason(NOW));
    }

    @Test
    @DisplayName("만료 시각이 지나면 거부한다")
    void rejectsExpired() {
        HouseholdInvite expired = invite(NOW.minusSeconds(1));
        assertEquals("만료된 초대 코드입니다", expired.unusableReason(NOW));
    }

    @Test
    @DisplayName("이미 사용된 코드는 재사용할 수 없다")
    void rejectsAlreadyUsed() {
        HouseholdInvite used = invite(NOW.plusHours(72));
        used.use(ACCEPTOR, NOW);

        assertEquals("이미 사용된 초대 코드입니다", used.unusableReason(NOW));
        assertEquals(ACCEPTOR, used.getUsedBy());
        assertEquals(NOW, used.getUsedAt());
    }

    @Test
    @DisplayName("회수된 코드는 만료 전이라도 거부한다")
    void rejectsRevoked() {
        HouseholdInvite revoked = invite(NOW.plusHours(72));
        revoked.revoke(NOW);

        assertEquals("회수된 초대 코드입니다", revoked.unusableReason(NOW));
        assertNotNull(revoked.getRevokedAt());
    }

    @Test
    @DisplayName("회수가 사용·만료보다 먼저 보고된다 — 회수된 코드는 다른 사유로 가려지지 않는다")
    void revokedTakesPrecedence() {
        HouseholdInvite invite = invite(NOW.minusHours(1));
        invite.use(ACCEPTOR, NOW.minusHours(2));
        invite.revoke(NOW);

        assertEquals("회수된 초대 코드입니다", invite.unusableReason(NOW));
    }
}

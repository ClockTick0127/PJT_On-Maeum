package com.nilm.device.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class HouseholdMemberTest {

    private static final UUID USER = UUID.randomUUID();

    private HouseholdMember member() {
        return new HouseholdMember("H001", USER, HouseholdMember.Relation.STAFF,
                HouseholdMember.NotifyPriority.SECONDARY, "010-1111-2222", true);
    }

    @Test
    @DisplayName("관계를 빠뜨리면 거부한다 — 본인과 담당자 사이에 기본값을 둘 수 없다")
    void relationIsRequired() {
        assertThrows(IllegalArgumentException.class, () ->
                new HouseholdMember("H001", USER, null, null, null, null));
    }

    @Test
    @DisplayName("관계만 주면 나머지는 기본값 — 순서는 PRIMARY, 수신은 켜짐")
    void appliesDefaultsOnNull() {
        HouseholdMember m = new HouseholdMember(
                "H001", USER, HouseholdMember.Relation.SELF, null, null, null);

        assertEquals(HouseholdMember.Relation.SELF, m.getRelation());
        assertEquals(HouseholdMember.NotifyPriority.PRIMARY, m.getNotifyPriority());
        assertTrue(m.isNotifyEnabled());
    }

    @Test
    @DisplayName("부분 수정: 넘긴 필드만 바뀌고 나머지는 유지된다")
    void updatesOnlyGivenFields() {
        HouseholdMember m = member();
        m.update(null, null, null, false);

        assertFalse(m.isNotifyEnabled(), "넘긴 필드는 반영");
        assertEquals(HouseholdMember.Relation.STAFF, m.getRelation(), "안 넘긴 관계는 유지");
        assertEquals(HouseholdMember.NotifyPriority.SECONDARY, m.getNotifyPriority(), "안 넘긴 순서는 유지");
        assertEquals("010-1111-2222", m.getNotifyPhone(), "안 넘긴 연락처는 유지");
    }

    @Test
    @DisplayName("부분 수정: 모든 필드를 넘기면 전부 바뀐다")
    void updatesAllFields() {
        HouseholdMember m = member();
        m.update(HouseholdMember.Relation.STAFF, HouseholdMember.NotifyPriority.PRIMARY,
                "010-9999-8888", false);

        assertEquals(HouseholdMember.Relation.STAFF, m.getRelation());
        assertEquals(HouseholdMember.NotifyPriority.PRIMARY, m.getNotifyPriority());
        assertEquals("010-9999-8888", m.getNotifyPhone());
        assertFalse(m.isNotifyEnabled());
    }

    @Test
    @DisplayName("복합 키는 가구와 사용자가 모두 같아야 동일하다")
    void compositeKeyEquality() {
        var id = new HouseholdMember.MemberId("H001", USER);

        assertEquals(id, new HouseholdMember.MemberId("H001", USER));
        assertEquals(id.hashCode(), new HouseholdMember.MemberId("H001", USER).hashCode());
        assertNotEquals(id, new HouseholdMember.MemberId("H002", USER));
        assertNotEquals(id, new HouseholdMember.MemberId("H001", UUID.randomUUID()));
    }
}

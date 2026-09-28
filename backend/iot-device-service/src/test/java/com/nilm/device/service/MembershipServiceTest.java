package com.nilm.device.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.nilm.device.common.DuplicateResourceException;
import com.nilm.device.common.ForbiddenException;
import com.nilm.device.common.NotFoundException;
import com.nilm.device.domain.Household;
import com.nilm.device.domain.HouseholdMember;
import com.nilm.device.repository.HouseholdMemberRepository;
import com.nilm.device.repository.HouseholdRepository;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MembershipServiceTest {

    private static final String HOUSE = "H001";
    private static final UUID USER = UUID.randomUUID();
    private static final UUID OTHER = UUID.randomUUID();

    @Mock
    private HouseholdMemberRepository memberRepository;

    @Mock
    private HouseholdRepository householdRepository;

    @InjectMocks
    private MembershipService service;

    private HouseholdMember member(UUID userId, HouseholdMember.Relation relation) {
        return new HouseholdMember(HOUSE, userId, relation,
                HouseholdMember.NotifyPriority.PRIMARY, "010-0000-0000", true);
    }

    @Test
    @DisplayName("가구 멤버가 아니면 접근이 거부된다")
    void deniesNonMember() {
        when(householdRepository.existsById(HOUSE)).thenReturn(true);
        when(memberRepository.existsById(any())).thenReturn(false);

        assertThrows(ForbiddenException.class, () -> service.requireMember(HOUSE, USER));
    }

    @Test
    @DisplayName("없는 가구는 권한 확인 이전에 404로 처리된다")
    void unknownHouseholdIsNotFound() {
        when(householdRepository.existsById(HOUSE)).thenReturn(false);

        assertThrows(NotFoundException.class, () -> service.requireMember(HOUSE, USER));
    }

    @Test
    @DisplayName("이미 연결된 사용자는 중복 등록되지 않는다")
    void rejectsDuplicateMembership() {
        when(memberRepository.existsById(any())).thenReturn(true);

        assertThrows(DuplicateResourceException.class, () -> service.add(
                HOUSE, USER, HouseholdMember.Relation.STAFF,
                HouseholdMember.NotifyPriority.SECONDARY, "010-1111-2222"));
        verify(memberRepository, never()).save(any());
    }

    @Test
    @DisplayName("마지막 남은 멤버는 해제할 수 없다 — 가구가 아무도 접근 못 하는 상태가 되는 것을 막는다")
    void cannotRemoveLastMember() {
        HouseholdMember only = member(USER, HouseholdMember.Relation.SELF);
        when(householdRepository.existsById(HOUSE)).thenReturn(true);
        when(memberRepository.existsById(any())).thenReturn(true);
        when(memberRepository.findById(any())).thenReturn(java.util.Optional.of(only));
        when(memberRepository.findByHouseIdOrderByNotifyPriorityAscJoinedAtAsc(HOUSE))
                .thenReturn(List.of(only));

        assertThrows(ForbiddenException.class, () -> service.remove(HOUSE, USER, USER));
        verify(memberRepository, never()).delete(any());
    }

    @Test
    @DisplayName("멤버가 둘 이상이면 해제할 수 있다")
    void removesWhenOtherMemberRemains() {
        HouseholdMember target = member(USER, HouseholdMember.Relation.STAFF);
        when(householdRepository.existsById(HOUSE)).thenReturn(true);
        when(memberRepository.existsById(any())).thenReturn(true);
        when(memberRepository.findById(any())).thenReturn(java.util.Optional.of(target));
        when(memberRepository.findByHouseIdOrderByNotifyPriorityAscJoinedAtAsc(HOUSE))
                .thenReturn(List.of(target, member(OTHER, HouseholdMember.Relation.STAFF)));

        service.remove(HOUSE, USER, OTHER);

        verify(memberRepository).delete(target);
    }

    @Test
    @DisplayName("내 가구 목록에 가구 별칭이 함께 나온다")
    void myHouseholdsIncludesAlias() {
        when(memberRepository.findByKeycloakUserIdOrderByJoinedAtAsc(USER))
                .thenReturn(List.of(member(USER, HouseholdMember.Relation.SELF)));
        when(householdRepository.findAllById(List.of(HOUSE)))
                .thenReturn(List.of(new Household(HOUSE, "우리 집", 120)));

        var result = service.myHouseholds(USER);

        assertEquals(1, result.size());
        assertEquals("우리 집", result.get(0).alias());
        assertEquals(HouseholdMember.Relation.SELF, result.get(0).relation());
    }

    @Test
    @DisplayName("가구 정보를 찾지 못해도 목록 조회가 실패하지 않는다 — 별칭만 비어서 나온다")
    void myHouseholdsToleratesMissingHousehold() {
        when(memberRepository.findByKeycloakUserIdOrderByJoinedAtAsc(USER))
                .thenReturn(List.of(member(USER, HouseholdMember.Relation.STAFF)));
        when(householdRepository.findAllById(List.of(HOUSE))).thenReturn(List.of());

        var result = service.myHouseholds(USER);

        assertEquals(1, result.size());
        assertEquals(null, result.get(0).alias());
    }
}

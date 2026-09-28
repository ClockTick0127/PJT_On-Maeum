package com.nilm.device.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.nilm.device.api.dto.InviteDtos;
import com.nilm.device.common.InvalidOperationException;
import com.nilm.device.common.NotFoundException;
import com.nilm.device.domain.HouseholdInvite;
import com.nilm.device.domain.HouseholdMember;
import com.nilm.device.repository.HouseholdInviteRepository;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class InviteServiceTest {

    private static final String HOUSE = "H001";
    private static final UUID CREATOR = UUID.randomUUID();
    private static final UUID ACCEPTOR = UUID.randomUUID();

    @Mock
    private HouseholdInviteRepository inviteRepository;

    @Mock
    private MembershipService membershipService;

    @InjectMocks
    private InviteService service;

    private HouseholdInvite invite(OffsetDateTime expiresAt) {
        return new HouseholdInvite("ABCD2345", HOUSE,
                HouseholdMember.Relation.STAFF, CREATOR, expiresAt);
    }

    @Test
    @DisplayName("발급 코드는 혼동하기 쉬운 문자(0·O·1·I·L)를 쓰지 않는 8자리다")
    void generatesUnambiguousCode() {
        when(inviteRepository.existsById(anyString())).thenReturn(false);
        when(inviteRepository.save(any())).thenAnswer(i -> i.getArgument(0));

        ArgumentCaptor<HouseholdInvite> saved = ArgumentCaptor.forClass(HouseholdInvite.class);
        service.create(HOUSE, new InviteDtos.CreateRequest(
                HouseholdMember.Relation.STAFF, 24), CREATOR);
        verify(inviteRepository).save(saved.capture());

        String code = saved.getValue().getCode();
        assertEquals(8, code.length());
        assertTrue(code.matches("[ABCDEFGHJKMNPQRSTUVWXYZ23456789]{8}"), "실제 코드: " + code);
    }

    @Test
    @DisplayName("만료 시간을 지정하지 않으면 기본 72시간이 적용된다")
    void appliesDefaultExpiry() {
        when(inviteRepository.existsById(anyString())).thenReturn(false);
        when(inviteRepository.save(any())).thenAnswer(i -> i.getArgument(0));

        OffsetDateTime before = OffsetDateTime.now(ZoneOffset.UTC);
        var response = service.create(HOUSE,
                new InviteDtos.CreateRequest(HouseholdMember.Relation.STAFF, null), CREATOR);

        assertTrue(response.expiresAt().isAfter(before.plusHours(71)));
        assertTrue(response.expiresAt().isBefore(before.plusHours(73)));
    }

    @Test
    @DisplayName("가구 멤버가 아니면 코드를 발급할 수 없다")
    void onlyMemberCanCreate() {
        doThrow(new com.nilm.device.common.ForbiddenException("권한 없음"))
                .when(membershipService).requireMember(eq(HOUSE), any());

        assertThrows(com.nilm.device.common.ForbiddenException.class, () -> service.create(
                HOUSE, new InviteDtos.CreateRequest(HouseholdMember.Relation.STAFF, 24), CREATOR));
        verify(inviteRepository, never()).save(any());
    }

    @Test
    @DisplayName("수락하면 코드의 관계대로 멤버십이 생기고 코드는 즉시 소비된다")
    void acceptCreatesMembershipAndConsumesCode() {
        HouseholdInvite target = invite(OffsetDateTime.now(ZoneOffset.UTC).plusHours(10));
        when(inviteRepository.findById("ABCD2345")).thenReturn(Optional.of(target));
        when(membershipService.add(any(), any(), any(), any(), any()))
                .thenReturn(new HouseholdMember(HOUSE, ACCEPTOR, HouseholdMember.Relation.STAFF,
                        HouseholdMember.NotifyPriority.SECONDARY, "010-1111-2222", true));

        var member = service.accept("ABCD2345", new InviteDtos.AcceptRequest("010-1111-2222"), ACCEPTOR);

        assertEquals(HouseholdMember.Relation.STAFF, member.relation());
        assertNotNull(target.getUsedAt(), "코드가 소비되어야 한다");
        assertEquals(ACCEPTOR, target.getUsedBy());
        verify(membershipService).add(HOUSE, ACCEPTOR,
                HouseholdMember.Relation.STAFF,
                HouseholdMember.NotifyPriority.SECONDARY, "010-1111-2222");
    }

    @Test
    @DisplayName("소문자로 입력해도 같은 코드로 인식한다")
    void acceptIsCaseInsensitive() {
        HouseholdInvite target = invite(OffsetDateTime.now(ZoneOffset.UTC).plusHours(10));
        when(inviteRepository.findById("ABCD2345")).thenReturn(Optional.of(target));
        when(membershipService.add(any(), any(), any(), any(), any()))
                .thenReturn(new HouseholdMember(HOUSE, ACCEPTOR, HouseholdMember.Relation.STAFF,
                        HouseholdMember.NotifyPriority.SECONDARY, null, true));

        service.accept("abcd2345", new InviteDtos.AcceptRequest(null), ACCEPTOR);

        assertNotNull(target.getUsedAt());
    }

    @Test
    @DisplayName("만료된 코드는 수락되지 않고 멤버십도 생기지 않는다")
    void rejectsExpiredCode() {
        HouseholdInvite expired = invite(OffsetDateTime.now(ZoneOffset.UTC).minusHours(1));
        when(inviteRepository.findById("ABCD2345")).thenReturn(Optional.of(expired));

        assertThrows(InvalidOperationException.class, () -> service.accept(
                "ABCD2345", new InviteDtos.AcceptRequest(null), ACCEPTOR));
        verify(membershipService, never()).add(any(), any(), any(), any(), any());
    }

    @Test
    @DisplayName("없는 코드는 404로 처리된다")
    void rejectsUnknownCode() {
        when(inviteRepository.findById(anyString())).thenReturn(Optional.empty());

        assertThrows(NotFoundException.class, () -> service.accept(
                "NOTEXIST", new InviteDtos.AcceptRequest(null), ACCEPTOR));
    }

    @Test
    @DisplayName("이미 사용된 코드는 회수할 수 없다")
    void cannotRevokeUsedCode() {
        HouseholdInvite used = invite(OffsetDateTime.now(ZoneOffset.UTC).plusHours(10));
        used.use(ACCEPTOR, OffsetDateTime.now(ZoneOffset.UTC));
        when(inviteRepository.findById("ABCD2345")).thenReturn(Optional.of(used));

        assertThrows(InvalidOperationException.class, () -> service.revoke("ABCD2345", CREATOR));
    }

    @Test
    @DisplayName("미사용 코드는 회수되어 이후 사용이 막힌다")
    void revokeBlocksLaterUse() {
        HouseholdInvite target = invite(OffsetDateTime.now(ZoneOffset.UTC).plusHours(10));
        when(inviteRepository.findById("ABCD2345")).thenReturn(Optional.of(target));

        var revoked = service.revoke("ABCD2345", CREATOR);

        assertNotNull(revoked.revokedAt());
        assertEquals("회수된 초대 코드입니다",
                target.unusableReason(OffsetDateTime.now(ZoneOffset.UTC)));
    }
}

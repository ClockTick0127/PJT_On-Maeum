package com.nilm.device.service;

import com.nilm.device.api.dto.InviteDtos;
import com.nilm.device.api.dto.MemberDtos;
import com.nilm.device.common.InvalidOperationException;
import com.nilm.device.common.NotFoundException;
import com.nilm.device.domain.HouseholdInvite;
import com.nilm.device.domain.HouseholdMember;
import com.nilm.device.repository.HouseholdInviteRepository;
import java.security.SecureRandom;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 가구 초대 — 코드는 로그인 수단이 아니라 접근 신청권이다.
 * 코드를 가진 사람이 아니라, 그 코드를 사용한 "계정"에 권한이 생기므로
 * 누가 언제 들어왔는지가 항상 남고 개인 단위로 회수할 수 있다.
 */
@Service
@Transactional(readOnly = true)
public class InviteService {

    /** 혼동하기 쉬운 문자(0/O, 1/I/L) 제외 — 문자로 불러줘야 하는 상황 대비 */
    private static final String ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789";
    private static final int CODE_LENGTH = 8;
    private static final int DEFAULT_EXPIRY_HOURS = 72;

    private final HouseholdInviteRepository inviteRepository;
    private final MembershipService membershipService;
    private final SecureRandom random = new SecureRandom();

    public InviteService(HouseholdInviteRepository inviteRepository,
                         MembershipService membershipService) {
        this.inviteRepository = inviteRepository;
        this.membershipService = membershipService;
    }

    @Transactional
    public InviteDtos.Response create(String houseId, InviteDtos.CreateRequest request, UUID caller) {
        membershipService.requireMember(houseId, caller);
        int hours = request.expiresInHours() == null ? DEFAULT_EXPIRY_HOURS : request.expiresInHours();
        HouseholdInvite invite = inviteRepository.save(new HouseholdInvite(
                generateCode(), houseId, request.relation(), caller,
                OffsetDateTime.now(ZoneOffset.UTC).plusHours(hours)));
        return InviteDtos.Response.from(invite);
    }

    public List<InviteDtos.Response> list(String houseId, UUID caller) {
        membershipService.requireMember(houseId, caller);
        return inviteRepository.findByHouseIdOrderByCreatedAtDesc(houseId).stream()
                .map(InviteDtos.Response::from)
                .toList();
    }

    /**
     * 초대 수락 — 코드를 소비하고 그 계정에 가구 멤버십을 만든다.
     * 코드 소비와 멤버십 생성은 한 트랜잭션이라 "코드만 쓰이고 권한은 없는" 상태가 생기지 않는다.
     */
    @Transactional
    public MemberDtos.Response accept(String code, InviteDtos.AcceptRequest request, UUID userId) {
        HouseholdInvite invite = inviteRepository.findById(code.toUpperCase())
                .orElseThrow(() -> new NotFoundException("초대 코드", code));

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        String unusable = invite.unusableReason(now);
        if (unusable != null) {
            throw new InvalidOperationException(unusable);
        }

        HouseholdMember member = membershipService.add(
                invite.getHouseId(), userId, invite.getRelation(),
                HouseholdMember.NotifyPriority.SECONDARY, request.notifyPhone());
        invite.use(userId, now);
        return MemberDtos.Response.from(member);
    }

    /** 코드 회수 — 아직 사용되지 않은 코드를 무효화한다. */
    @Transactional
    public InviteDtos.Response revoke(String code, UUID caller) {
        HouseholdInvite invite = inviteRepository.findById(code.toUpperCase())
                .orElseThrow(() -> new NotFoundException("초대 코드", code));
        membershipService.requireMember(invite.getHouseId(), caller);
        if (invite.getUsedAt() != null) {
            throw new InvalidOperationException("이미 사용된 코드는 회수할 수 없습니다");
        }
        invite.revoke(OffsetDateTime.now(ZoneOffset.UTC));
        return InviteDtos.Response.from(invite);
    }

    private String generateCode() {
        for (int attempt = 0; attempt < 5; attempt++) {
            StringBuilder sb = new StringBuilder(CODE_LENGTH);
            for (int i = 0; i < CODE_LENGTH; i++) {
                sb.append(ALPHABET.charAt(random.nextInt(ALPHABET.length())));
            }
            String code = sb.toString();
            if (!inviteRepository.existsById(code)) {
                return code;
            }
        }
        throw new InvalidOperationException("초대 코드 생성에 실패했습니다. 다시 시도해 주세요");
    }
}

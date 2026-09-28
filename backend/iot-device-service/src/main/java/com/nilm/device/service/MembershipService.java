package com.nilm.device.service;

import com.nilm.device.api.dto.MemberDtos;
import com.nilm.device.common.DuplicateResourceException;
import com.nilm.device.common.ForbiddenException;
import com.nilm.device.common.NotFoundException;
import com.nilm.device.domain.Household;
import com.nilm.device.domain.HouseholdMember;
import com.nilm.device.repository.HouseholdMemberRepository;
import com.nilm.device.repository.HouseholdRepository;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 가구 멤버십 — 계정과 가구를 잇는 유일한 연결점이자 접근 권한의 원본.
 * 이 가구의 데이터를 볼 수 있는가, 알림을 받는가가 모두 여기서 결정된다.
 */
@Service
@Transactional(readOnly = true)
public class MembershipService {

    private final HouseholdMemberRepository memberRepository;
    private final HouseholdRepository householdRepository;

    public MembershipService(HouseholdMemberRepository memberRepository,
                             HouseholdRepository householdRepository) {
        this.memberRepository = memberRepository;
        this.householdRepository = householdRepository;
    }

    /** 가구 멤버 목록 — PRIMARY 우선 정렬(알림 발송 순서 기준) */
    public List<MemberDtos.Response> list(String houseId, UUID caller) {
        requireMember(houseId, caller);
        return memberRepository.findByHouseIdOrderByNotifyPriorityAscJoinedAtAsc(houseId).stream()
                .map(MemberDtos.Response::from)
                .toList();
    }

    /** 내가 접근 가능한 가구 목록 (가구 별칭 포함) */
    public List<MemberDtos.MyHousehold> myHouseholds(UUID userId) {
        List<HouseholdMember> members = memberRepository.findByKeycloakUserIdOrderByJoinedAtAsc(userId);
        Map<String, Household> households = householdRepository
                .findAllById(members.stream().map(HouseholdMember::getHouseId).toList()).stream()
                .collect(Collectors.toMap(Household::getHouseId, Function.identity()));

        return members.stream()
                .map(m -> new MemberDtos.MyHousehold(
                        m.getHouseId(),
                        households.containsKey(m.getHouseId())
                                ? households.get(m.getHouseId()).getAlias() : null,
                        m.getRelation(),
                        m.getNotifyPriority(),
                        m.isNotifyEnabled()))
                .toList();
    }

    @Transactional
    public HouseholdMember add(String houseId, UUID userId, HouseholdMember.Relation relation,
                               HouseholdMember.NotifyPriority priority, String notifyPhone) {
        if (memberRepository.existsById(new HouseholdMember.MemberId(houseId, userId))) {
            throw new DuplicateResourceException("가구 멤버십", userId);
        }
        return memberRepository.save(
                new HouseholdMember(houseId, userId, relation, priority, notifyPhone, true));
    }

    @Transactional
    public MemberDtos.Response update(String houseId, UUID targetUserId,
                                      MemberDtos.UpdateRequest request, UUID caller) {
        requireMember(houseId, caller);
        HouseholdMember member = find(houseId, targetUserId);
        member.update(request.relation(), request.notifyPriority(),
                request.notifyPhone(), request.notifyEnabled());
        return MemberDtos.Response.from(member);
    }

    /** 접근 해제 — 마지막 멤버는 제거할 수 없다(가구가 고아가 되는 것을 막는다). */
    @Transactional
    public void remove(String houseId, UUID targetUserId, UUID caller) {
        requireMember(houseId, caller);
        HouseholdMember member = find(houseId, targetUserId);
        if (memberRepository.findByHouseIdOrderByNotifyPriorityAscJoinedAtAsc(houseId).size() <= 1) {
            throw new ForbiddenException("가구의 마지막 멤버는 해제할 수 없습니다");
        }
        memberRepository.delete(member);
    }

    /** 이 가구에 대한 접근 권한 확인 — 멤버가 아니면 403 */
    public void requireMember(String houseId, UUID userId) {
        if (!householdRepository.existsById(houseId)) {
            throw new NotFoundException("가구", houseId);
        }
        if (!memberRepository.existsById(new HouseholdMember.MemberId(houseId, userId))) {
            throw new ForbiddenException("해당 가구에 대한 권한이 없습니다: " + houseId);
        }
    }

    private HouseholdMember find(String houseId, UUID userId) {
        return memberRepository.findById(new HouseholdMember.MemberId(houseId, userId))
                .orElseThrow(() -> new NotFoundException("가구 멤버십", userId));
    }
}

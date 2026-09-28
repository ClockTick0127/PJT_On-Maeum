package com.nilm.device.service;

import com.nilm.device.api.dto.HouseholdDtos;
import com.nilm.device.common.DuplicateResourceException;
import com.nilm.device.common.NotFoundException;
import com.nilm.device.domain.Household;
import com.nilm.device.domain.HouseholdMember;
import com.nilm.device.domain.UserProfile;
import com.nilm.device.repository.HouseholdRepository;
import com.nilm.device.repository.UserProfileRepository;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional(readOnly = true)
public class HouseholdService {

    private final HouseholdRepository householdRepository;
    private final MembershipService membershipService;
    private final UserProfileRepository profileRepository;

    public HouseholdService(HouseholdRepository householdRepository,
                            MembershipService membershipService,
                            UserProfileRepository profileRepository) {
        this.householdRepository = householdRepository;
        this.membershipService = membershipService;
        this.profileRepository = profileRepository;
    }

    /**
     * 가구 등록 — 생성자를 첫 멤버(PRIMARY)로 함께 등록한다.
     * 이렇게 해야 생성 직후부터 접근 권한과 초대 코드 발급 주체가 존재한다.
     * 관계는 프로필의 기관 소속 여부로 정한다: 소속이 있으면 STAFF(대신 등록), 없으면 SELF(본인 가구).
     */
    @Transactional
    public HouseholdDtos.Response create(HouseholdDtos.CreateRequest request, UUID creator) {
        if (householdRepository.existsById(request.houseId())) {
            throw new DuplicateResourceException("가구", request.houseId());
        }
        Household saved = householdRepository.saveAndFlush(
                new Household(request.houseId(), request.alias(), request.graceMinutes()));

        UserProfile profile = profileRepository.findById(creator)
                .orElseThrow(() -> new NotFoundException("프로필", creator));
        HouseholdMember.Relation relation =
                (profile.getOrganization() == null || profile.getOrganization().isBlank())
                        ? HouseholdMember.Relation.SELF
                        : HouseholdMember.Relation.STAFF;
        membershipService.add(saved.getHouseId(), creator, relation,
                HouseholdMember.NotifyPriority.PRIMARY, profile.getPhone());

        return HouseholdDtos.Response.from(saved);
    }

    public HouseholdDtos.Response get(String houseId, UUID caller) {
        membershipService.requireMember(houseId, caller);
        Household household = householdRepository.findById(houseId)
                .orElseThrow(() -> new NotFoundException("가구", houseId));
        return HouseholdDtos.Response.from(household);
    }

    /** 관리 목적의 전체 조회 — 일반 사용자는 /api/auth/me의 households를 사용한다. */
    public List<HouseholdDtos.Response> list() {
        return householdRepository.findAll().stream()
                .map(HouseholdDtos.Response::from)
                .toList();
    }
}

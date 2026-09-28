package com.nilm.device.repository;

import com.nilm.device.domain.HouseholdMember;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HouseholdMemberRepository
        extends JpaRepository<HouseholdMember, HouseholdMember.MemberId> {

    /** 알림 발송 순서(PRIMARY 우선)로 정렬 — 모니터링의 수신자 조회가 사용 */
    List<HouseholdMember> findByHouseIdOrderByNotifyPriorityAscJoinedAtAsc(String houseId);

    /** 내가 속한 가구 목록 — 로그인 직후 화면 구성에 사용 */
    List<HouseholdMember> findByKeycloakUserIdOrderByJoinedAtAsc(UUID keycloakUserId);
}

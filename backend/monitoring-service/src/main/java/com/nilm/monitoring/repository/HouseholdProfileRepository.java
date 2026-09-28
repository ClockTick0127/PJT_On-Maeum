package com.nilm.monitoring.repository;

import com.nilm.monitoring.domain.HouseholdProfile;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HouseholdProfileRepository extends JpaRepository<HouseholdProfile, Long> {

    boolean existsByHouseholdIdAndProfileVersion(String householdId, String profileVersion);

    /**
     * 가구의 ACTIVE 프로필. 정상이라면 0행 또는 1행이지만,
     * 불변식이 깨진 경우에도 최신 한 벌을 고를 수 있도록 목록으로 받는다.
     */
    List<HouseholdProfile> findByHouseholdIdAndStatusOrderByEffectiveFromDescIdDesc(
            String householdId,
            HouseholdProfile.Status status
    );
}

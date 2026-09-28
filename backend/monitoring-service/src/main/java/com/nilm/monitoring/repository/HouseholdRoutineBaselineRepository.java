package com.nilm.monitoring.repository;

import com.nilm.monitoring.domain.HouseholdRoutineBaseline;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HouseholdRoutineBaselineRepository
        extends JpaRepository<HouseholdRoutineBaseline, Long> {

    List<HouseholdRoutineBaseline> findByProfileIdOrderByApplianceTypeAscBaselineScopeAscWeekdayAsc(
            Long profileId
    );
}

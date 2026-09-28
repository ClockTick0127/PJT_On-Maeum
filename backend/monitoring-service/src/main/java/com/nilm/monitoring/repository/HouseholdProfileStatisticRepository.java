package com.nilm.monitoring.repository;

import com.nilm.monitoring.domain.HouseholdProfileStatistic;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HouseholdProfileStatisticRepository
        extends JpaRepository<HouseholdProfileStatistic, Long> {

    List<HouseholdProfileStatistic> findByProfileId(Long profileId);
}

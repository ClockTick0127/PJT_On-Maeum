package com.nilm.monitoring.repository;

import com.nilm.monitoring.domain.HouseholdDailyApplianceUsage;
import com.nilm.monitoring.domain.HouseholdDailyApplianceUsageId;
import java.time.LocalDate;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HouseholdDailyApplianceUsageRepository
        extends JpaRepository<HouseholdDailyApplianceUsage, HouseholdDailyApplianceUsageId> {

    /** 평가가 읽는 "오늘"의 가전별 사용 사실. */
    List<HouseholdDailyApplianceUsage> findByHouseholdIdAndUsageDate(
            String householdId,
            LocalDate usageDate
    );
}

package com.nilm.monitoring.repository;

import com.nilm.monitoring.domain.HourlyPowerUsage;
import com.nilm.monitoring.domain.HourlyPowerUsageId;
import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HourlyPowerUsageRepository
        extends JpaRepository<HourlyPowerUsage, HourlyPowerUsageId> {

    List<HourlyPowerUsage>
    findAllByHouseholdIdAndBucketStartAtGreaterThanEqualAndBucketStartAtLessThanOrderByBucketStartAtAsc(
            String householdId,
            OffsetDateTime from,
            OffsetDateTime until
    );
}

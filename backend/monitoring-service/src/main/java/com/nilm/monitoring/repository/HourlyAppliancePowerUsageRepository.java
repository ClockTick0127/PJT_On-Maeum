package com.nilm.monitoring.repository;

import com.nilm.monitoring.domain.HourlyAppliancePowerUsage;
import com.nilm.monitoring.domain.HourlyAppliancePowerUsageId;
import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HourlyAppliancePowerUsageRepository
        extends JpaRepository<HourlyAppliancePowerUsage, HourlyAppliancePowerUsageId> {

    List<HourlyAppliancePowerUsage>
    findAllByHouseholdIdAndBucketStartAtGreaterThanEqualAndBucketStartAtLessThanOrderByApplianceTypeAscBucketStartAtAsc(
            String householdId,
            OffsetDateTime from,
            OffsetDateTime until
    );
}

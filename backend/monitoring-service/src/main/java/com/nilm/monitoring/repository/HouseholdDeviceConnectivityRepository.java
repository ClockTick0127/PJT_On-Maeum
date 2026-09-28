package com.nilm.monitoring.repository;

import com.nilm.monitoring.domain.HouseholdDeviceConnectivity;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HouseholdDeviceConnectivityRepository
        extends JpaRepository<HouseholdDeviceConnectivity, Long> {

    List<HouseholdDeviceConnectivity> findByHouseholdId(String householdId);
}

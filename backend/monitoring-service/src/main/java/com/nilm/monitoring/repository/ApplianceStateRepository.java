package com.nilm.monitoring.repository;

import com.nilm.monitoring.domain.ApplianceState;
import com.nilm.monitoring.domain.ApplianceStateId;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ApplianceStateRepository
        extends JpaRepository<ApplianceState, ApplianceStateId> {

    List<ApplianceState> findByHouseholdId(String householdId);
}

package com.nilm.monitoring.repository;

import com.nilm.monitoring.domain.HouseholdObservation;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HouseholdObservationRepository
        extends JpaRepository<HouseholdObservation, String> {
}

package com.nilm.device.repository;

import com.nilm.device.domain.Household;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HouseholdRepository extends JpaRepository<Household, String> {
}

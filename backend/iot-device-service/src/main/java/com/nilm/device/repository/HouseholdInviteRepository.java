package com.nilm.device.repository;

import com.nilm.device.domain.HouseholdInvite;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HouseholdInviteRepository extends JpaRepository<HouseholdInvite, String> {

    List<HouseholdInvite> findByHouseIdOrderByCreatedAtDesc(String houseId);
}

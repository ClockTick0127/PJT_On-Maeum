package com.nilm.device.repository;

import com.nilm.device.domain.UserProfile;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserProfileRepository extends JpaRepository<UserProfile, UUID> {

    boolean existsByEmail(String email);

    Optional<UserProfile> findByEmail(String email);
}

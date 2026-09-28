package com.nilm.device.repository;

import com.nilm.device.domain.ManagerRegistrationOutbox;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ManagerRegistrationOutboxRepository
        extends JpaRepository<ManagerRegistrationOutbox, UUID> {

    /** 먼저 적힌 가입부터 내보낸다. */
    List<ManagerRegistrationOutbox> findByStatusAndNextAttemptAtLessThanEqualOrderByCreatedAt(
            ManagerRegistrationOutbox.Status status, OffsetDateTime now, Pageable page);

    boolean existsByKeycloakUserId(UUID keycloakUserId);
}

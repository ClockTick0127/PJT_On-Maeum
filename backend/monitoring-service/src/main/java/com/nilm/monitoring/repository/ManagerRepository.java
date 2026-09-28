package com.nilm.monitoring.repository;

import com.nilm.monitoring.domain.Manager;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface ManagerRepository
        extends JpaRepository<Manager, Long> {

    Optional<Manager> findByAuthSub(String authSub); // JWT sub와 담당자 auth_sub 연결

    boolean existsByEmailIgnoreCaseAndIdNot(String email, Long id);

    boolean existsByEmailIgnoreCase(String email);
}

package com.nilm.device.repository;

import com.nilm.device.domain.InstallHistory;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface InstallHistoryRepository extends JpaRepository<InstallHistory, Long> {

    List<InstallHistory> findByDeviceIdOrderByChangedAtDesc(Long deviceId);
}

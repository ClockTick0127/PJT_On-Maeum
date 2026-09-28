package com.nilm.device.repository;

import com.nilm.device.domain.DeviceCredential;
import com.nilm.device.domain.DeviceStatus;
import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface DeviceCredentialRepository extends JpaRepository<DeviceCredential, Long> {

    boolean existsByMqttUsername(String mqttUsername);

    /** passwd 동기화 대상: 폐기되지 않았고 기기 상태가 허용 목록에 있는 계정 */
    @Query("""
            select c from DeviceCredential c, Device d
            where d.deviceId = c.deviceId
              and c.revokedAt is null
              and d.status in :statuses
            order by c.mqttUsername
            """)
    List<DeviceCredential> findSyncTargets(@Param("statuses") Collection<DeviceStatus> statuses);
}

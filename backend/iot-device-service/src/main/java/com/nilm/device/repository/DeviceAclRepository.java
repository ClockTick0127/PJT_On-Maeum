package com.nilm.device.repository;

import com.nilm.device.domain.DeviceAcl;
import com.nilm.device.domain.DeviceStatus;
import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface DeviceAclRepository extends JpaRepository<DeviceAcl, Long> {

    List<DeviceAcl> findByDeviceId(Long deviceId);

    /** ACL 파일 동기화 대상: 유효 계정(미폐기) + 기기 상태 허용 목록 */
    @Query("""
            select new com.nilm.device.repository.AclSyncRow(c.mqttUsername, a.topicPattern, a.permission)
            from DeviceAcl a, Device d, DeviceCredential c
            where a.deviceId = d.deviceId
              and c.deviceId = d.deviceId
              and c.revokedAt is null
              and d.status in :statuses
            order by c.mqttUsername, a.topicPattern
            """)
    List<AclSyncRow> findSyncTargets(@Param("statuses") Collection<DeviceStatus> statuses);
}

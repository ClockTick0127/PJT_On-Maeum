package com.nilm.device.repository;

import com.nilm.device.domain.Device;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DeviceRepository extends JpaRepository<Device, Long> {

    List<Device> findByHouseId(String houseId);
}

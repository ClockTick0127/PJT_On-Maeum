package com.nilm.monitoring.service;

import com.nilm.monitoring.domain.HouseholdDeviceConnectivity;
import com.nilm.monitoring.dto.kafka.DeviceConnectionChangedMessage;
import com.nilm.monitoring.repository.HouseholdDeviceConnectivityRepository;
import java.time.OffsetDateTime;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 가구 기기 접속 상태 — 오경보 방지 판정의 근거.
 *
 * <p>무활동 위험은 "사람이 아무것도 안 썼다"는 뜻인데, 기기가 꺼져 있었다면
 * 그건 <b>사람에 대한 신호가 아니라 기기에 대한 신호</b>다. 구분하지 못하면
 * Wi-Fi가 끊길 때마다 보호자에게 위험 알림이 간다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DeviceConnectivityService {

    private final HouseholdDeviceConnectivityRepository repository;

    @Transactional
    public void receive(DeviceConnectionChangedMessage message) {
        if (message.deviceId() == null || message.houseId() == null
                || message.status() == null || message.occurredAt() == null) {
            log.warn("필드가 비어 무시: {}", message);
            return;
        }
        HouseholdDeviceConnectivity.Status status;
        try {
            status = HouseholdDeviceConnectivity.Status.valueOf(message.status());
        } catch (IllegalArgumentException e) {
            log.warn("알 수 없는 접속 상태 무시: {}", message.status());
            return;
        }

        repository.findById(message.deviceId()).ifPresentOrElse(
                existing -> {
                    if (existing.applyIfNewer(status, message.occurredAt())) {
                        log.info("기기 접속 상태 갱신: deviceId={}, {}", message.deviceId(), status);
                    } else {
                        log.debug("늦게 도착한 옛 사건 무시: deviceId={}, occurredAt={}",
                                message.deviceId(), message.occurredAt());
                    }
                },
                () -> repository.save(new HouseholdDeviceConnectivity(
                        message.deviceId(), message.houseId(), status, message.occurredAt())));
    }

    /**
     * 그 시각에 가구의 기기가 끊겨 있었는지.
     *
     * <p>한 가구에 기기가 여러 대일 수 있으므로 <b>하나라도 살아 있었으면 정상</b>으로 본다.
     * 기기 하나가 꺼졌다고 무활동 판정을 통째로 버리면 진짜 위험을 놓친다.
     *
     * <p>기록이 아예 없으면 <b>끊긴 것으로 보지 않는다.</b> LWT를 아직 붙이지 않은 기기가
     * 있을 수 있는데, 모른다는 이유로 위험 알림을 막으면 안 된다.
     * 안부 확인에서는 놓치는 쪽이 더 비싸다.
     */
    @Transactional(readOnly = true)
    public boolean wasDisconnectedAt(String householdId, OffsetDateTime at) {
        List<HouseholdDeviceConnectivity> devices = repository.findByHouseholdId(householdId);
        if (devices.isEmpty()) {
            return false;
        }
        boolean anyKnown = false;
        for (HouseholdDeviceConnectivity d : devices) {
            // 사건보다 나중에 일어난 상태 변화는 그 시점의 판단 근거가 될 수 없다.
            if (d.getOccurredAt().isAfter(at)) {
                continue;
            }
            anyKnown = true;
            if (d.getConnectionStatus() == HouseholdDeviceConnectivity.Status.ONLINE) {
                return false;
            }
        }
        return anyKnown;
    }
}

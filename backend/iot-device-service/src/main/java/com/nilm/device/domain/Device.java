package com.nilm.device.domain;

import com.nilm.device.common.InvalidStateTransitionException;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Set;

@Entity
@Table(name = "devices")
public class Device {

    /** 허용된 상태 전이 — 이 맵에 없는 전이는 전부 거부 (RETIRED는 종착 상태) */
    private static final Map<DeviceStatus, Set<DeviceStatus>> ALLOWED_TRANSITIONS = Map.of(
            DeviceStatus.REGISTERED, Set.of(DeviceStatus.ACTIVE, DeviceStatus.RETIRED),
            DeviceStatus.ACTIVE, Set.of(DeviceStatus.SUSPENDED, DeviceStatus.RETIRED),
            DeviceStatus.SUSPENDED, Set.of(DeviceStatus.ACTIVE, DeviceStatus.RETIRED),
            DeviceStatus.RETIRED, Set.of()
    );

    /**
     * MQTT 접속 상태 — {@link DeviceStatus}(생명주기)와 독립이다.
     * 운영자가 정하는 것이 아니라 브로커가 알려 주는 사실이다.
     */
    public enum ConnectionStatus {
        /** 한 번도 붙은 적 없음 */
        UNKNOWN,
        ONLINE,
        /** LWT로 통지받았거나 정상 종료 — 데이터가 끊긴 이유가 기기 쪽임을 뜻한다 */
        OFFLINE
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "device_id")
    private Long deviceId;

    @Column(name = "house_id", nullable = false, length = 10)
    private String houseId;

    @Enumerated(EnumType.STRING)
    @Column(name = "device_type", nullable = false, length = 20)
    private DeviceType deviceType;

    @Column(length = 50)
    private String location;

    @Column(name = "firmware_ver", length = 20)
    private String firmwareVer;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 15)
    private DeviceStatus status = DeviceStatus.REGISTERED;

    @Enumerated(EnumType.STRING)
    @Column(name = "connection_status", nullable = false, length = 10)
    private ConnectionStatus connectionStatus = ConnectionStatus.UNKNOWN;

    /** 마지막으로 온라인을 확인한 시각. 오프라인이 되어도 지우지 않는다. */
    @Column(name = "last_seen_at")
    private OffsetDateTime lastSeenAt;

    @Column(name = "connection_changed_at")
    private OffsetDateTime connectionChangedAt;

    @Column(name = "registered_at", nullable = false, updatable = false, insertable = false)
    private OffsetDateTime registeredAt;

    protected Device() {
    }

    public Device(String houseId, DeviceType deviceType, String location, String firmwareVer) {
        this.houseId = houseId;
        this.deviceType = deviceType;
        this.location = location;
        this.firmwareVer = firmwareVer;
    }

    /** 상태 전이 — 허용 목록에 없으면 예외. 호출자는 install_history 기록 책임을 가진다. */
    public DeviceStatus transitionTo(DeviceStatus target) {
        if (!ALLOWED_TRANSITIONS.getOrDefault(status, Set.of()).contains(target)) {
            throw new InvalidStateTransitionException(status.name(), target.name());
        }
        DeviceStatus from = this.status;
        this.status = target;
        return from;
    }

    public Long getDeviceId() {
        return deviceId;
    }

    public String getHouseId() {
        return houseId;
    }

    public DeviceType getDeviceType() {
        return deviceType;
    }

    public String getLocation() {
        return location;
    }

    public String getFirmwareVer() {
        return firmwareVer;
    }

    public DeviceStatus getStatus() {
        return status;
    }

    /**
     * 브로커가 알려 준 접속 상태를 반영한다.
     *
     * <p>같은 상태가 반복 통지되면 {@code connectionChangedAt}을 갱신하지 않는다.
     * "언제부터 끊겨 있었나"를 알아야 하는데, 재통지마다 시각을 밀면 그 값이 사라진다.
     *
     * @return 상태가 실제로 바뀌었으면 true
     */
    public boolean updateConnection(ConnectionStatus next, OffsetDateTime at) {
        if (next == null || at == null) {
            throw new IllegalArgumentException("접속 상태와 시각은 필수입니다");
        }
        if (next == ConnectionStatus.ONLINE) {
            this.lastSeenAt = at;
        }
        if (this.connectionStatus == next) {
            return false;
        }
        this.connectionStatus = next;
        this.connectionChangedAt = at;
        return true;
    }

    public ConnectionStatus getConnectionStatus() {
        return connectionStatus;
    }

    public OffsetDateTime getLastSeenAt() {
        return lastSeenAt;
    }

    public OffsetDateTime getConnectionChangedAt() {
        return connectionChangedAt;
    }

    public OffsetDateTime getRegisteredAt() {
        return registeredAt;
    }
}

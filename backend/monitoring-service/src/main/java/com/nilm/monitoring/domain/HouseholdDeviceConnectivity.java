package com.nilm.monitoring.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 가구 기기의 MQTT 접속 상태 — device-service가 LWT로 감지해 보내 준다.
 *
 * <p>데이터가 끊긴 원인이 사람의 무활동인지 기기 장애인지 가르는 데 쓴다.
 * device_db와 FK로 묶지 않고 {@code household_id} 값만 공유한다.
 */
@Entity
@Table(name = "household_device_connectivity")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class HouseholdDeviceConnectivity {

    public enum Status {
        UNKNOWN, ONLINE, OFFLINE
    }

    @Id
    @Column(name = "device_id")
    private Long deviceId;

    @Column(name = "household_id", nullable = false)
    private String householdId;

    @Column(name = "connection_status", nullable = false, length = 10)
    @jakarta.persistence.Enumerated(jakarta.persistence.EnumType.STRING)
    private Status connectionStatus;

    /** 기기가 실제로 그 상태가 된 시각. 수신 시각이 아니다. */
    @Column(name = "occurred_at", nullable = false)
    private OffsetDateTime occurredAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    public HouseholdDeviceConnectivity(Long deviceId, String householdId,
                                       Status status, OffsetDateTime occurredAt) {
        this.deviceId = deviceId;
        this.householdId = householdId;
        this.connectionStatus = status;
        this.occurredAt = occurredAt;
        this.updatedAt = OffsetDateTime.now();
    }

    /**
     * 더 최근 사건만 반영한다.
     *
     * <p>Kafka는 재전송이 있고 파티션이 달라지면 순서가 어긋날 수 있다.
     * 늦게 도착한 옛 사건이 최신 상태를 덮으면, 살아 있는 기기를 죽은 것으로
     * 판정해 오히려 오경보를 만든다.
     */
    public boolean applyIfNewer(Status status, OffsetDateTime at) {
        if (at.isBefore(occurredAt)) {
            return false;
        }
        this.connectionStatus = status;
        this.occurredAt = at;
        this.updatedAt = OffsetDateTime.now();
        return true;
    }
}

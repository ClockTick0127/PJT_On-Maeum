package com.nilm.device.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;

@Entity
@Table(name = "install_history")
public class InstallHistory {

    public enum EventType {
        INSTALLED, RELOCATED, STATUS_CHANGED, FIRMWARE_UPDATED, RETIRED, CREDENTIAL_ROTATED
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "history_id")
    private Long historyId;

    @Column(name = "device_id", nullable = false)
    private Long deviceId;

    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false, length = 20)
    private EventType eventType;

    @Column(name = "from_value", length = 50)
    private String fromValue;

    @Column(name = "to_value", length = 50)
    private String toValue;

    @Column(length = 200)
    private String reason;

    @Column(name = "changed_by", length = 50)
    private String changedBy;

    @Column(name = "changed_at", nullable = false, updatable = false, insertable = false)
    private OffsetDateTime changedAt;

    protected InstallHistory() {
    }

    public InstallHistory(Long deviceId, EventType eventType, String fromValue, String toValue,
                          String reason, String changedBy) {
        this.deviceId = deviceId;
        this.eventType = eventType;
        this.fromValue = fromValue;
        this.toValue = toValue;
        this.reason = reason;
        this.changedBy = changedBy;
    }

    public Long getHistoryId() {
        return historyId;
    }

    public Long getDeviceId() {
        return deviceId;
    }

    public EventType getEventType() {
        return eventType;
    }

    public String getFromValue() {
        return fromValue;
    }

    public String getToValue() {
        return toValue;
    }

    public String getReason() {
        return reason;
    }

    public String getChangedBy() {
        return changedBy;
    }

    public OffsetDateTime getChangedAt() {
        return changedAt;
    }
}

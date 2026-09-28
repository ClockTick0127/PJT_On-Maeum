package com.nilm.device.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@Entity
@Table(name = "device_credentials")
public class DeviceCredential {

    @Id
    @Column(name = "device_id")
    private Long deviceId;

    @Column(name = "mqtt_username", nullable = false, unique = true, length = 50)
    private String mqttUsername;

    @Column(name = "secret_hash", nullable = false, length = 200)
    private String secretHash;

    /** Mosquitto passwd 파일용 브로커 호환 해시 ($7$...) — 동기화 시 파일에 기록됨 */
    @Column(name = "mosquitto_hash", length = 300)
    private String mosquittoHash;

    @Column(name = "issued_at", nullable = false, updatable = false, insertable = false)
    private OffsetDateTime issuedAt;

    @Column(name = "rotated_at")
    private OffsetDateTime rotatedAt;

    @Column(name = "revoked_at")
    private OffsetDateTime revokedAt;

    protected DeviceCredential() {
    }

    public DeviceCredential(Long deviceId, String mqttUsername, String secretHash, String mosquittoHash) {
        this.deviceId = deviceId;
        this.mqttUsername = mqttUsername;
        this.secretHash = secretHash;
        this.mosquittoHash = mosquittoHash;
    }

    public void revoke() {
        this.revokedAt = OffsetDateTime.now(ZoneOffset.UTC);
    }

    /** 비밀번호 로테이션 — 새 해시로 교체하고 rotated_at 기록. username은 유지. */
    public void rotate(String newSecretHash, String newMosquittoHash) {
        this.secretHash = newSecretHash;
        this.mosquittoHash = newMosquittoHash;
        this.rotatedAt = OffsetDateTime.now();
    }

    public Long getDeviceId() {
        return deviceId;
    }

    public String getMqttUsername() {
        return mqttUsername;
    }

    public String getSecretHash() {
        return secretHash;
    }

    public String getMosquittoHash() {
        return mosquittoHash;
    }

    public OffsetDateTime getIssuedAt() {
        return issuedAt;
    }

    public OffsetDateTime getRotatedAt() {
        return rotatedAt;
    }

    public OffsetDateTime getRevokedAt() {
        return revokedAt;
    }
}

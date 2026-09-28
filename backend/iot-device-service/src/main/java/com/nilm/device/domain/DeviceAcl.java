package com.nilm.device.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "device_acl")
public class DeviceAcl {

    public enum Permission {
        PUBLISH, SUBSCRIBE
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "acl_id")
    private Long aclId;

    @Column(name = "device_id", nullable = false)
    private Long deviceId;

    @Column(name = "topic_pattern", nullable = false, length = 100)
    private String topicPattern;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 10)
    private Permission permission;

    protected DeviceAcl() {
    }

    public DeviceAcl(Long deviceId, String topicPattern, Permission permission) {
        this.deviceId = deviceId;
        this.topicPattern = topicPattern;
        this.permission = permission;
    }

    public Long getAclId() {
        return aclId;
    }

    public Long getDeviceId() {
        return deviceId;
    }

    public String getTopicPattern() {
        return topicPattern;
    }

    public Permission getPermission() {
        return permission;
    }
}

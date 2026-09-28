package com.nilm.monitoring.domain;

import jakarta.persistence.*;
import lombok.Getter;

@Entity
@Getter
@Table(
        name = "notification_settings",
        uniqueConstraints = @UniqueConstraint(
                name = "uk_notification_setting",
                columnNames = {"manager_id", "notification_type", "channel"}
        )
)
public class NotificationSetting {

    public enum Type {
        DANGER, // 위험 알림 수신
        WARNING, // 주의 알림 수신
        DAILY_SUMMARY // 9시 데일리 요약 알림 수신
    }

    public enum Channel {
        PUSH
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "manager_id", nullable = false)
    private Long managerId;

    @Enumerated(EnumType.STRING)
    @Column(name = "notification_type", nullable = false)
    private Type notificationType = Type.DANGER;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Channel channel = Channel.PUSH;

    @Column(nullable = false)
    private boolean enabled = true;

    protected NotificationSetting() {
    }

    public NotificationSetting(
            Long managerId,
            Type notificationType,
            Channel channel,
            boolean enabled
    ) {
        this.managerId = managerId;
        this.notificationType = notificationType;
        this.channel = channel;
        this.enabled = enabled;
    }

    public void changeEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}

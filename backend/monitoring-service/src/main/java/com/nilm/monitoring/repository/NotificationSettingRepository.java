package com.nilm.monitoring.repository;

import com.nilm.monitoring.domain.NotificationSetting;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface NotificationSettingRepository
        extends JpaRepository<NotificationSetting, Long> {

    Optional<NotificationSetting> findByManagerIdAndNotificationTypeAndChannel(
            Long managerId,
            NotificationSetting.Type notificationType,
            NotificationSetting.Channel channel
    );
}

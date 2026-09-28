package com.nilm.monitoring.service;

import com.nilm.monitoring.config.enums.RiskLevel;
import com.nilm.monitoring.domain.NotificationSetting;
import com.nilm.monitoring.domain.Subject;
import com.nilm.monitoring.repository.NotificationSettingRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * 담당자의 알림 수신 설정을 확인한다.
 *
 * <p>설정이 꺼져 있어도 알림 행은 만든다. 담당자 화면의 "미해결 사건"과 이상 징후 기록은
 * 발송 여부와 무관하게 사실 그대로 남아야 하기 때문이다. 꺼져 있을 때 막는 것은 발송뿐이다.
 */
@Component
@RequiredArgsConstructor
public class NotificationGate {

    private final NotificationSettingRepository settings;

    /**
     * @return 이 등급의 알림을 이 채널로 보내도 되면 true.
     *         설정을 따로 만든 적이 없으면 기본 수신으로 본다.
     */
    public boolean allows(Subject subject, RiskLevel level, NotificationSetting.Channel channel) {
        if (subject.getManagerId() == null) {
            return true;
        }

        NotificationSetting.Type type = switch (level) {
            case DANGER -> NotificationSetting.Type.DANGER;
            case WARNING -> NotificationSetting.Type.WARNING;
            case NORMAL -> null;
        };
        if (type == null) {
            return true;
        }

        return settings
                .findByManagerIdAndNotificationTypeAndChannel(subject.getManagerId(), type, channel)
                .map(NotificationSetting::isEnabled)
                .orElse(true);
    }
}

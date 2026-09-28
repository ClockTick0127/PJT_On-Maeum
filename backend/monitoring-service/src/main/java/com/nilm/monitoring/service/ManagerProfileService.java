package com.nilm.monitoring.service;

import com.nilm.monitoring.domain.Manager;
import com.nilm.monitoring.domain.NotificationSetting;
import com.nilm.monitoring.dto.ManagerProfileUpdateRequest;
import com.nilm.monitoring.repository.ManagerRepository;
import com.nilm.monitoring.repository.NotificationSettingRepository;
import java.util.Locale;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
@RequiredArgsConstructor
public class ManagerProfileService {

    private final ManagerRepository managers;
    private final NotificationSettingRepository notificationSettings;

    @Transactional
    public void update(String authSub, ManagerProfileUpdateRequest request) {
        if (authSub == null || authSub.isBlank()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "로그인이 필요합니다.");
        }
        if (!request.hasChanges()) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "수정할 항목을 하나 이상 입력해야 합니다."
            );
        }

        Manager manager = managers.findByAuthSub(authSub)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.FORBIDDEN,
                        "등록된 담당자만 계정을 수정할 수 있습니다."
                ));

        String email = normalizeEmail(request.email());
        if (email != null && managers.existsByEmailIgnoreCaseAndIdNot(email, manager.getId())) {
            throw new ResponseStatusException(
                    HttpStatus.CONFLICT,
                    "이미 사용 중인 이메일입니다."
            );
        }

        String organization = request.organization() == null
                ? null
                : request.organization().strip();
        manager.updateProfile(email, organization);

        if (request.pushEnabled() != null) {
            updateNotificationSetting(
                    manager.getId(), NotificationSetting.Type.DANGER, request.pushEnabled());
            updateNotificationSetting(
                    manager.getId(), NotificationSetting.Type.WARNING, request.pushEnabled());
        }
        if (request.dailyReportEnabled() != null) {
            updateNotificationSetting(
                    manager.getId(),
                    NotificationSetting.Type.DAILY_SUMMARY,
                    request.dailyReportEnabled()
            );
        }
    }

    private void updateNotificationSetting(
            Long managerId,
            NotificationSetting.Type type,
            boolean enabled
    ) {
        NotificationSetting setting = notificationSettings
                .findByManagerIdAndNotificationTypeAndChannel(
                        managerId, type, NotificationSetting.Channel.PUSH)
                .orElseGet(() -> new NotificationSetting(
                        managerId, type, NotificationSetting.Channel.PUSH, enabled));
        setting.changeEnabled(enabled);
        notificationSettings.save(setting);
    }

    private String normalizeEmail(String email) {
        return email == null ? null : email.strip().toLowerCase(Locale.ROOT);
    }
}

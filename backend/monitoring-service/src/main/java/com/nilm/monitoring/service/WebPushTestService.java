package com.nilm.monitoring.service;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
@Service
@Profile("local")
@ConditionalOnProperty(name = "app.push.enabled", havingValue = "true")
@RequiredArgsConstructor
public class WebPushTestService {
    private final NotificationService notificationService;
    private final NotificationDispatchService dispatcher;
    public Map<String, Object> sendTest() {
        var notification = notificationService.createNotification(null, true);
        return dispatcher.dispatch(new NotificationReady(notification.getId(),
                "안전 확인 테스트", NotificationReady.RESPONSE_QUESTION + " 예 또는 아니오로 응답해 주세요."));
    }
}


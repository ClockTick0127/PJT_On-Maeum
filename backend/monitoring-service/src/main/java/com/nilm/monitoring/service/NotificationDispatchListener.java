package com.nilm.monitoring.service;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.push.enabled", havingValue = "true")
public class NotificationDispatchListener {
    private final NotificationDispatchService dispatcher;
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onReady(NotificationReady event) {
        log.info("알림 발송 요청 수신: notificationId={}, 제목={}",
                event.notificationId(), event.title());
        try {
            var result = dispatcher.dispatch(event);
            log.info("알림 발송 후처리 완료: {}", result);
        } catch (Exception e) {
            // DB 커밋 이후 실패를 Kafka 처리 실패로 위장하지 않는다.
            log.error("알림 발송 후처리 실패: notificationId={}", event.notificationId(), e);
        }
    }
}


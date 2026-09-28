package com.nilm.monitoring.service;

import com.nilm.monitoring.domain.Notification;
import com.nilm.monitoring.repository.NotificationRepository;
import com.nilm.monitoring.repository.PushSubscriptionRepository;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.push.enabled", havingValue = "true")
public class NotificationDispatchService {
    private final NotificationRepository notifications;
    private final PushSubscriptionRepository subscriptions;
    private final NotificationService notificationService;
    private final WebPushSender sender;

    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public Map<String, Object> dispatch(NotificationReady request) {
        Notification notification = notifications.findById(request.notificationId())
                .orElseThrow(() -> new IllegalArgumentException("알림이 없습니다."));
        var targets = subscriptions.findAllByAuthSub(notification.getAuthSub());
        log.info("웹푸시 발송 시작: notificationId={}, 제목={}, 구독 수={}, 응답요구={}",
                notification.getId(), request.title(), targets.size(),
                notification.getResponseStatus() == Notification.ResponseStatus.PENDING);
        if (targets.isEmpty()) {
            // 구독이 없으면 보낼 곳이 없다. "알림은 떴는데 폰에 안 온다"의 첫 번째 원인이다.
            log.warn("웹푸시 구독이 없어 발송 대상 없음: notificationId={}", notification.getId());
        }
        Map<String, Object> payload = new HashMap<>();
        payload.put("title", request.title());
        payload.put("body", request.body());
        payload.put("url", "/");
        if (notification.getResponseStatus() == Notification.ResponseStatus.PENDING) {
            payload.put("notificationId", notification.getId().toString());
            payload.put("expiresAt", notification.getResponseDeadline().toString());
        }
        int accepted = 0;
        for (var target : targets) {
            long ttl = notification.getResponseDeadline() == null ? 300 :
                    Duration.between(OffsetDateTime.now(ZoneOffset.UTC),
                            notification.getResponseDeadline()).getSeconds();
            if (ttl <= 0) {
                log.warn("응답 기한이 지나 남은 구독으로는 발송하지 않음: notificationId={}, 기한={}",
                        notification.getId(), notification.getResponseDeadline());
                break;
            }
            try {
                int status = sender.send(target, payload, (int) Math.min(ttl, 300));
                if (status >= 200 && status < 300) {
                    accepted++;
                    log.info("푸시 접수 성공: notificationId={}, subscriptionId={}, status={}",
                            notification.getId(), target.getId(), status);
                } else if (status == 404 || status == 410) {
                    subscriptions.deleteById(target.getId());
                    log.info("만료된 구독 삭제: notificationId={}, subscriptionId={}, status={}",
                            notification.getId(), target.getId(), status);
                } else {
                    log.warn("푸시 접수 실패: subscriptionId={}, status={}", target.getId(), status);
                }
            } catch (InterruptedException e) {
                log.warn("푸시 발송 중단: notificationId={}, subscriptionId={}",
                        notification.getId(), target.getId());
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.warn("푸시 전송 실패: subscriptionId={}, errorType={}",
                        target.getId(), e.getClass().getSimpleName());
            }
        }
        notificationService.updateSendResult(notification.getId(), accepted > 0);
        log.info("웹푸시 발송 종료: notificationId={}, 구독 수={}, 접수 수={}, 결과={}",
                notification.getId(), targets.size(), accepted,
                accepted > 0 ? "성공" : "실패");
        return Map.of("notificationId", notification.getId(),
                "subscriptionCount", targets.size(), "acceptedCount", accepted);
    }
}


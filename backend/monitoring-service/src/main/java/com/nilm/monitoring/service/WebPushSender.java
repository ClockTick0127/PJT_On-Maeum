package com.nilm.monitoring.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nilm.monitoring.domain.PushSubscription;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.martijndwars.webpush.PushService;
import org.springframework.stereotype.Service;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import java.net.URI;
import java.util.Map;

@Slf4j
@Service
@ConditionalOnProperty(name = "app.push.enabled", havingValue = "true")
@RequiredArgsConstructor
public class WebPushSender {

    private final PushService pushService;
    private final ObjectMapper objectMapper;

    public int send(
            PushSubscription subscription,
            Map<String, Object> payload,
            int ttlSeconds
    ) throws Exception {

        byte[] body = objectMapper.writeValueAsBytes(payload);

        // DB 엔티티 Notification과 이름이 같아 전체 패키지명 사용
        var message = new nl.martijndwars.webpush.Notification(
                subscription.getEndpoint(),
                subscription.getP256dh(),
                subscription.getAuth(),
                body,
                ttlSeconds
        );

        // 엔드포인트 전체 주소는 그 자체가 발송 자격이라 남기지 않고 푸시 서버만 남긴다.
        String pushServer = pushServerOf(subscription.getEndpoint());
        log.info("웹푸시 전송 시도: subscriptionId={}, 푸시서버={}, TTL={}초, 본문={}바이트",
                subscription.getId(), pushServer, ttlSeconds, body.length);

        try {
            var response = pushService.send(message);
            int status = response.getStatusLine().getStatusCode();
            log.info("웹푸시 전송 응답: subscriptionId={}, 푸시서버={}, status={}",
                    subscription.getId(), pushServer, status);
            return status;
        } catch (InterruptedException e) {
            log.warn("웹푸시 전송 중단: subscriptionId={}, 푸시서버={}",
                    subscription.getId(), pushServer);
            Thread.currentThread().interrupt();
            throw e;
        } catch (Exception e) {
            log.warn("웹푸시 전송 예외: subscriptionId={}, 푸시서버={}, 예외={}",
                    subscription.getId(), pushServer, e.getClass().getSimpleName());
            throw e;
        }
    }

    /** 로그에 남겨도 되는 만큼만. 주소를 못 읽으면 알 수 없음으로 둔다. */
    private String pushServerOf(String endpoint) {
        try {
            return URI.create(endpoint).getHost();
        } catch (Exception e) {
            return "알 수 없음";
        }
    }
}

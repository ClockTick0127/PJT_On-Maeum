package com.nilm.device.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nilm.device.domain.ManagerRegistrationOutbox;
import com.nilm.device.repository.ManagerRegistrationOutboxRepository;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.function.Consumer;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 아웃박스에 쌓인 담당자 가입을 Kafka로 내보낸다.
 *
 * <p>발행과 상태 갱신이 한 원자 단위가 아니다. 보낸 직후 죽으면 같은 이벤트가 다시
 * 나가지만, 잃지는 않는다. 받는 쪽이 auth_sub로 멱등하게 처리하는 것을 전제한다.
 *
 * <p>Key를 사용자 ID로 고정해 한 사람의 이벤트가 같은 파티션에서 순서대로 처리되게 한다.
 */
@Component
@ConditionalOnProperty(
        name = "app.manager-registration.publisher.enabled",
        havingValue = "true",
        matchIfMissing = true)
public class ManagerRegistrationPublisher {

    private static final Logger log = LoggerFactory.getLogger(ManagerRegistrationPublisher.class);

    private final ManagerRegistrationOutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactions;
    private final String topic;
    private final int batchSize;
    private final Duration retryAfter;
    private final Duration sendTimeout;

    public ManagerRegistrationPublisher(
            ManagerRegistrationOutboxRepository outbox,
            KafkaTemplate<String, String> kafka,
            ObjectMapper objectMapper,
            TransactionTemplate transactions,
            @Value("${app.kafka.manager-registered-topic:device.manager-registered.v1}") String topic,
            @Value("${app.manager-registration.publisher.batch-size:100}") int batchSize,
            @Value("${app.manager-registration.publisher.retry-after:30s}") Duration retryAfter,
            @Value("${app.manager-registration.publisher.send-timeout:10s}") Duration sendTimeout) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.objectMapper = objectMapper;
        this.transactions = transactions;
        this.topic = topic;
        this.batchSize = batchSize;
        this.retryAfter = retryAfter;
        this.sendTimeout = sendTimeout;
    }

    @Scheduled(fixedDelayString = "${app.manager-registration.publisher.interval-ms:5000}")
    public void publishPending() {
        OffsetDateTime now = OffsetDateTime.now();
        List<ManagerRegistrationOutbox> pending =
                outbox.findByStatusAndNextAttemptAtLessThanEqualOrderByCreatedAt(
                        ManagerRegistrationOutbox.Status.PENDING, now,
                        PageRequest.of(0, batchSize));

        for (ManagerRegistrationOutbox row : pending) {
            publishOne(row, now);
        }
    }

    private void publishOne(ManagerRegistrationOutbox row, OffsetDateTime now) {
        try {
            String payload = objectMapper.writeValueAsString(new ManagerRegisteredMessage(
                    row.getEventId(),
                    row.getKeycloakUserId().toString(),
                    row.getDisplayName(),
                    row.getOrganization(),
                    row.getPhone(),
                    row.getEmail(),
                    row.getCreatedAt()));

            kafka.send(topic, row.getKeycloakUserId().toString(), payload)
                    .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);

            update(row, published -> published.markPublished(now));
            log.info("담당자 가입 발행: topic={}, authSub={}, eventId={}",
                    topic, row.getKeycloakUserId(), row.getEventId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            recordFailure(row, now, e);
        } catch (Exception e) {
            recordFailure(row, now, e);
        }
    }

    private void recordFailure(ManagerRegistrationOutbox row, OffsetDateTime now, Exception e) {
        // 행이 남아 있으니 다음 주기에 다시 시도한다. 가입은 이미 성공한 뒤다.
        log.warn("담당자 가입 발행 실패 — {} 뒤 재시도: authSub={}, eventId={}",
                retryAfter, row.getKeycloakUserId(), row.getEventId(), e);
        update(row, failed -> failed.markFailed(now, retryAfter, e.toString()));
    }

    private void update(ManagerRegistrationOutbox row, Consumer<ManagerRegistrationOutbox> change) {
        transactions.executeWithoutResult(status -> outbox.findById(row.getEventId())
                .filter(found -> found.getStatus() == ManagerRegistrationOutbox.Status.PENDING)
                .ifPresent(change));
    }
}

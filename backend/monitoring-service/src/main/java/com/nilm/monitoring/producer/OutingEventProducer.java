package com.nilm.monitoring.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nilm.monitoring.dto.kafka.OutingEventMessage;
import com.nilm.monitoring.service.HouseholdPresenceChanged;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * 커밋된 외출 상태 변경만 AI 분석 서비스로 내보낸다.
 *
 * <p>롤백된 트랜잭션의 외출 상태가 AI에 남으면 감시가 잘못 보류되므로
 * 커밋 이후에만 발행한다. 대신 커밋과 발행이 한 원자 단위는 아니어서,
 * 발행에 실패한 변경은 유실된다(계약상 AI는 at-least-once 수신을 전제로 한다).
 * 유실까지 막아야 하면 Transactional Outbox로 옮겨야 한다.
 *
 * <p>Key를 {@code household_id}로 고정해 한 가구의 시작·종료가 같은 파티션에서
 * 보낸 순서대로 처리되게 한다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutingEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.outing-event-topic:monitoring.household-presence.v1}")
    private String topic;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onPresenceChanged(HouseholdPresenceChanged event) {
        OutingEventMessage message = new OutingEventMessage(
                event.eventId(),
                event.householdId(),
                event.eventType(),
                event.occurredAt()
        );

        String payload;
        try {
            payload = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            log.error(
                    "외출 이벤트 직렬화 실패: householdId={}, eventId={}",
                    event.householdId(),
                    event.eventId(),
                    e
            );
            return;
        }

        try {
            kafkaTemplate.send(topic, event.householdId(), payload)
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            log.error(
                                    "외출 이벤트 발행 실패: topic={}, householdId={}, eventId={}",
                                    topic,
                                    event.householdId(),
                                    event.eventId(),
                                    error
                            );
                            return;
                        }
                        log.info(
                                "외출 이벤트 발행: householdId={}, eventType={}, occurredAt={}, eventId={}",
                                event.householdId(),
                                event.eventType(),
                                event.occurredAt(),
                                event.eventId()
                        );
                    });
        } catch (Exception e) {
            // 커밋은 이미 끝났다. 발행 실패를 외출 설정 API 실패로 위장하지 않는다.
            log.error(
                    "외출 이벤트 발행 요청 실패: topic={}, householdId={}, eventId={}",
                    topic,
                    event.householdId(),
                    event.eventId(),
                    e
            );
        }
    }
}

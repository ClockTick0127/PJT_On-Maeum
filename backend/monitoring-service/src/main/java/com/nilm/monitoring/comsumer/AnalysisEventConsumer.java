package com.nilm.monitoring.comsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nilm.monitoring.dto.kafka.AnalysisEventMessage;
import com.nilm.monitoring.service.AnalysisEventService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;
import org.springframework.kafka.annotation.KafkaListener;

@Slf4j
@Component @RequiredArgsConstructor
public class AnalysisEventConsumer {

    private final AnalysisEventService analysisEventService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${app.kafka.analysis-event-topic:analysis.event.v1}")
    public void consume(ConsumerRecord<String, String> record) throws JsonProcessingException {
        AnalysisEventMessage event = objectMapper.readValue(
                record.value(),
                AnalysisEventMessage.class
        );

        log.info(
                "분석 이벤트 수신: eventId={}, householdId={}, eventType={}, applianceType={}, "
                        + "occurredAt={}, partition={}, offset={}",
                event.eventId(),
                event.householdId(),
                event.eventType(),
                event.applianceType(),
                event.occurredAt(),
                record.partition(),
                record.offset()
        );

        // 이후 이벤트 저장 서비스 호출 위치
        analysisEventService.handle(event);

        log.info("분석 이벤트 처리 완료: eventId={}, householdId={}",
                event.eventId(), event.householdId());
    }
}

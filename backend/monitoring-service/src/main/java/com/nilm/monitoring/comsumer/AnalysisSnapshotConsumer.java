package com.nilm.monitoring.comsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nilm.monitoring.dto.kafka.AnalysisSnapshotMessage;
import com.nilm.monitoring.service.ApplianceActivityService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 가전 ON/OFF 스냅샷을 받는다.
 * 이상 징후와 달리 상시로 들어오는 흐름이라 건별 로그는 debug로만 남긴다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AnalysisSnapshotConsumer {

    private final ApplianceActivityService applianceActivityService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${app.kafka.analysis-snapshot-topic:analysis.snapshot.v1}",
            groupId = "${app.kafka.analysis-snapshot-group-id:monitoring-service-analysis-snapshot-v1}"
    )
    public void consume(ConsumerRecord<String, String> record) throws JsonProcessingException {
        AnalysisSnapshotMessage snapshot = objectMapper.readValue(
                record.value(),
                AnalysisSnapshotMessage.class
        );

        log.debug(
                "분석 스냅샷 수신: snapshotId={}, householdId={}, observedAt={}, partition={}, offset={}",
                snapshot.snapshotId(),
                snapshot.householdId(),
                snapshot.observedAt(),
                record.partition(),
                record.offset()
        );

        applianceActivityService.handle(snapshot);
    }
}

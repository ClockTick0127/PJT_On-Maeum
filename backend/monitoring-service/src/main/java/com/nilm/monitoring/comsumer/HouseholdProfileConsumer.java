package com.nilm.monitoring.comsumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nilm.monitoring.dto.kafka.HouseholdProfileMessage;
import com.nilm.monitoring.service.HouseholdProfileService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Gold 배치가 만든 가구 프로필을 받는다. 가구·버전당 한 건이라 건별로 로그를 남긴다.
 *
 * <p>이상 징후 소비와 달리 어떤 메시지로도 예외를 올리지 않는다.
 * 오류 핸들러가 컨테이너를 멈추므로, 한 가구의 깨진 프로필 때문에
 * 나머지 가구의 프로필 갱신까지 멈춰서는 안 된다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class HouseholdProfileConsumer {

    private final HouseholdProfileService householdProfileService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${app.kafka.household-profile-topic:gold.household-profile.v1}",
            groupId = "${app.kafka.household-profile-group-id:monitoring-service-household-profile-v1}"
    )
    public void consume(ConsumerRecord<String, String> record) {
        HouseholdProfileMessage profile;
        try {
            profile = objectMapper.readValue(record.value(), HouseholdProfileMessage.class);
        } catch (Exception e) {
            log.warn("읽을 수 없는 프로필 메시지 무시: key={}, partition={}, offset={}",
                    record.key(), record.partition(), record.offset(), e);
            return;
        }

        log.debug("가구 프로필 수신: householdId={}, profileVersion={}, asOfDate={}, partition={}, offset={}",
                profile.householdId(),
                profile.profileVersion(),
                profile.asOfDate(),
                record.partition(),
                record.offset());

        householdProfileService.receive(profile);
    }
}

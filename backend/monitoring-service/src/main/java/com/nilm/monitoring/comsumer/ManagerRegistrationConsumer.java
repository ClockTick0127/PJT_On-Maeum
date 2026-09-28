package com.nilm.monitoring.comsumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nilm.monitoring.dto.kafka.ManagerRegisteredMessage;
import com.nilm.monitoring.service.ManagerRegistrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 가입한 복지사를 담당자 명단에 반영한다.
 *
 * <p>프로필 소비와 같은 이유로 어떤 메시지로도 예외를 올리지 않는다. 오류 핸들러가
 * 컨테이너를 멈추므로, 한 사람의 깨진 메시지가 뒤이은 가입까지 막아서는 안 된다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ManagerRegistrationConsumer {

    private final ManagerRegistrationService managerRegistrationService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${app.kafka.manager-registered-topic:device.manager-registered.v1}",
            groupId = "${app.kafka.manager-registered-group-id:monitoring-service-manager-registered-v1}"
    )
    public void consume(ConsumerRecord<String, String> record) {
        ManagerRegisteredMessage message;
        try {
            message = objectMapper.readValue(record.value(), ManagerRegisteredMessage.class);
        } catch (Exception e) {
            log.warn("읽을 수 없는 담당자 가입 메시지 무시: key={}, partition={}, offset={}",
                    record.key(), record.partition(), record.offset(), e);
            return;
        }

        try {
            managerRegistrationService.receive(message);
        } catch (Exception e) {
            // 다음 재전송이나 운영자의 개입에 맡긴다. 컨테이너는 계속 돈다.
            log.error("담당자 등록 실패: authSub={}, eventId={}, partition={}, offset={}",
                    message.authSub(), message.eventId(), record.partition(), record.offset(), e);
        }
    }
}

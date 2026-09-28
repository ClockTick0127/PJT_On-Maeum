package com.nilm.monitoring.comsumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nilm.monitoring.dto.kafka.DeviceConnectionChangedMessage;
import com.nilm.monitoring.service.DeviceConnectivityService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 기기 접속 상태 변화 수신 — 오경보 방지 판정에 쓴다.
 *
 * <p>다른 소비자와 같은 이유로 어떤 메시지로도 예외를 올리지 않는다.
 * 한 기기의 깨진 메시지가 뒤이은 통지까지 막아서는 안 된다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceConnectionConsumer {

    private final DeviceConnectivityService service;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${app.kafka.device-connection-topic:device.connection-changed.v1}",
            groupId = "${app.kafka.device-connection-group-id:monitoring-service-device-connection-v1}"
    )
    public void consume(ConsumerRecord<String, String> record) {
        DeviceConnectionChangedMessage message;
        try {
            message = objectMapper.readValue(record.value(), DeviceConnectionChangedMessage.class);
        } catch (Exception e) {
            log.warn("읽을 수 없는 기기 접속 메시지 무시: key={}, partition={}, offset={}",
                    record.key(), record.partition(), record.offset(), e);
            return;
        }
        try {
            service.receive(message);
        } catch (Exception e) {
            log.error("기기 접속 상태 반영 실패: deviceId={}, partition={}, offset={}",
                    message.deviceId(), record.partition(), record.offset(), e);
        }
    }
}

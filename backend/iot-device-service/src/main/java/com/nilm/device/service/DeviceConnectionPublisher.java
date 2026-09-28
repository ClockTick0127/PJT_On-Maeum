package com.nilm.device.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nilm.device.domain.Device;
import java.time.OffsetDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * 기기 접속 상태 변화를 monitoring에 알린다.
 *
 * <p>접속 상태를 device_db에 적어두기만 하면 <b>아무도 읽지 않는 컬럼</b>이 된다.
 * 오경보 방지는 위험 판정을 하는 monitoring 쪽에서 일어나야 하는데, 두 서비스는
 * DB를 공유하지 않으므로 이벤트로 잇는다. 담당자 가입과 같은 방식이다.
 *
 * <p><b>발행 실패는 삼킨다.</b> 접속 상태는 부가 정보이고, 이것 때문에 상태 기록
 * 트랜잭션이 깨지면 안 된다. 대신 놓친 전이는 monitoring 쪽에서 복구할 수 없으므로
 * 재시작 시 현재 상태를 다시 발행한다(아래 {@link #republishCurrent}).
 */
@Component
public class DeviceConnectionPublisher {

    private static final Logger log = LoggerFactory.getLogger(DeviceConnectionPublisher.class);

    /**
     * 기기 접속 상태 변화.
     *
     * @param houseId     monitoring은 기기가 아니라 가구 단위로 판정하므로 함께 보낸다
     * @param occurredAt  판정 기준 시각 — 이벤트가 늦게 도착해도 같은 답이 나와야 한다
     */
    public record ConnectionChanged(
            Long deviceId,
            String houseId,
            String status,
            OffsetDateTime occurredAt) {
    }

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper objectMapper;
    private final String topic;
    private final boolean enabled;

    public DeviceConnectionPublisher(
            KafkaTemplate<String, String> kafka,
            ObjectMapper objectMapper,
            @Value("${app.kafka.device-connection-topic:device.connection-changed.v1}") String topic,
            @Value("${app.kafka.device-connection-publish-enabled:true}") boolean enabled) {
        this.kafka = kafka;
        this.objectMapper = objectMapper;
        this.topic = topic;
        this.enabled = enabled;
    }

    public void publish(Device device, OffsetDateTime occurredAt) {
        if (!enabled) {
            return;
        }
        ConnectionChanged event = new ConnectionChanged(
                device.getDeviceId(), device.getHouseId(),
                device.getConnectionStatus().name(), occurredAt);
        try {
            // key = houseId — monitoring이 가구 단위로 처리하므로 같은 가구는 순서가 보장돼야 한다
            kafka.send(topic, device.getHouseId(), objectMapper.writeValueAsString(event));
            log.debug("기기 접속 상태 발행: {}", event);
        } catch (JsonProcessingException e) {
            log.error("접속 상태 직렬화 실패: deviceId={}", device.getDeviceId(), e);
        } catch (RuntimeException e) {
            // 브로커가 죽어 있어도 상태 기록 자체는 남아야 한다
            log.warn("접속 상태 발행 실패 — 상태는 DB에 기록됨: deviceId={}, {}",
                    device.getDeviceId(), e.getMessage());
        }
    }
}

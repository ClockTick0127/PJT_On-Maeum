package com.nilm.monitoring.config;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * Kafka 리스너 컨테이너가 실제로 돌고 있는지를 readiness에 노출한다.
 *
 * <p>컨테이너가 멈추면 이벤트는 토픽에 쌓이기만 하고 알림은 나가지 않는다.
 * 그 상태를 헬스 체크가 모르면 "알림이 안 온다"는 증상만 남고 원인은 로그를 뒤져야 나온다.
 *
 * <p>자동 시작이 꺼진 컨테이너({@code KAFKA_CONSUMER_ENABLED=false}, 테스트)는
 * 멈춰 있는 것이 정상이므로 DOWN으로 보지 않는다.
 */
@Component("kafkaListenersHealthIndicator")
@RequiredArgsConstructor
public class KafkaListenerHealthIndicator implements HealthIndicator {

    private final KafkaListenerEndpointRegistry registry;

    @Override
    public Health health() {
        Map<String, Object> details = new LinkedHashMap<>();
        boolean allRunning = true;
        for (MessageListenerContainer container : registry.getAllListenerContainers()) {
            String id = container.getListenerId();
            if (!container.isAutoStartup()) {
                details.put(id, "DISABLED");
                continue;
            }
            if (container.isRunning()) {
                details.put(id, container.isContainerPaused() ? "PAUSED" : "RUNNING");
                continue;
            }
            details.put(id, "STOPPED");
            allRunning = false;
        }
        Health.Builder builder = allRunning ? Health.up() : Health.down();
        return builder.withDetails(details).build();
    }
}

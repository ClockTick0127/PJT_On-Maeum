package com.nilm.monitoring.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

class KafkaListenerHealthIndicatorTest {

    private static MessageListenerContainer container(String id, boolean autoStartup, boolean running) {
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        when(container.getListenerId()).thenReturn(id);
        when(container.isAutoStartup()).thenReturn(autoStartup);
        when(container.isRunning()).thenReturn(running);
        when(container.isContainerPaused()).thenReturn(false);
        return container;
    }

    private static KafkaListenerHealthIndicator indicator(MessageListenerContainer... containers) {
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        when(registry.getAllListenerContainers()).thenReturn(List.of(containers));
        return new KafkaListenerHealthIndicator(registry);
    }

    @Test
    void 모든_리스너가_돌고_있으면_UP() {
        Health health = indicator(
                container("analysis-event", true, true),
                container("analysis-snapshot", true, true)
        ).health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails())
                .containsEntry("analysis-event", "RUNNING")
                .containsEntry("analysis-snapshot", "RUNNING");
    }

    @Test
    void 자동_시작_리스너가_멈춰_있으면_DOWN이고_어느_리스너인지_보인다() {
        Health health = indicator(
                container("analysis-event", true, false),
                container("analysis-snapshot", true, true)
        ).health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails())
                .containsEntry("analysis-event", "STOPPED")
                .containsEntry("analysis-snapshot", "RUNNING");
    }

    @Test
    void 자동_시작이_꺼진_리스너는_멈춰_있어도_UP() {
        // KAFKA_CONSUMER_ENABLED=false 배포와 테스트 컨텍스트가 여기에 해당한다.
        Health health = indicator(container("analysis-event", false, false)).health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("analysis-event", "DISABLED");
    }
}

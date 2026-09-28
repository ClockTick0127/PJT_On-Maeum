package com.nilm.monitoring.scene;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.scene-demo.enabled", havingValue = "true")
public class SceneSnapshotConsumer {
    private final SceneSnapshotService service;

    @KafkaListener(topics = "${app.scene-demo.topic:analysis.scene.v2}",
            autoStartup = "${app.scene-demo.auto-startup:${spring.kafka.listener.auto-startup:true}}",
            groupId = "${app.scene-demo.group-id:monitoring-selected-scene-v2}")
    public void consume(String body) throws JsonProcessingException {
        service.accept(body);
    }
}

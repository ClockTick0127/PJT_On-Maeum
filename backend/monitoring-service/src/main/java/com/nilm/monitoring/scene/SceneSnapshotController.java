package com.nilm.monitoring.scene;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.scene-demo.enabled", havingValue = "true")
@RequestMapping("/api/monitoring/admin/selected-scene")
public class SceneSnapshotController {
    private final SceneSnapshotService service;

    @GetMapping
    public ResponseEntity<JsonNode> read(@RequestParam String householdId,
            @RequestParam String runId, @RequestParam String profileId,
            @RequestParam(required = false) Long sourceIndex) {
        return ResponseEntity.of(service.read(householdId, runId, profileId, sourceIndex));
    }
}

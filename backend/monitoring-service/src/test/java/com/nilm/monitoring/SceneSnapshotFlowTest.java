package com.nilm.monitoring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nilm.monitoring.scene.SceneSnapshotConsumer;
import com.nilm.monitoring.scene.SceneSnapshotRepository;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(properties = "app.scene-demo.enabled=true")
@AutoConfigureMockMvc
class SceneSnapshotFlowTest {
    @Autowired SceneSnapshotConsumer consumer;
    @Autowired SceneSnapshotRepository repository;
    @Autowired ObjectMapper mapper;
    @Autowired MockMvc mvc;

    @BeforeEach void clear() { repository.deleteAll(); }

    ObjectNode snapshot(long index) {
        ObjectNode node = mapper.createObjectNode();
        node.put("schema_version", 2).put("scope", "selected_scene")
                .put("snapshot_id", UUID.randomUUID().toString()).put("household_id", "r3-test")
                .put("run_id", "run-1").put("profile_id", "kettle-profile")
                .put("source_index", index).put("observed_at", "2026-09-21T00:00:00Z")
                .put("published_at", "2026-09-21T00:00:01Z")
                .put("target_appliance", "KETTLE").put("ready", true);
        var items = node.putArray("appliances");
        for (String name : List.of("KETTLE", "INDUCTION", "IRON", "MICROWAVE", "HAIR_DRYER", "VACUUM_CLEANER")) {
            var item = items.addObject().put("appliance_type", name);
            if (name.equals("KETTLE")) {
                item.put("inferred", true).put("probability", .999).put("state", "ON").put("is_on", true);
            } else {
                item.put("inferred", false).putNull("probability").put("state", "UNKNOWN").putNull("is_on");
            }
        }
        return node;
    }

    @Test void persistsUnknownAndScoreThroughApiAndIgnoresIdenticalRetry() throws Exception {
        String body = snapshot(10).toString();
        consumer.consume(body);
        consumer.consume(body);
        assertThat(repository.count()).isEqualTo(1);
        mvc.perform(get("/api/monitoring/admin/selected-scene")
                .param("householdId", "r3-test").param("runId", "run-1").param("profileId", "kettle-profile"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.appliances[0].probability").value(.999))
                .andExpect(jsonPath("$.appliances[1].state").value("UNKNOWN"))
                .andExpect(jsonPath("$.appliances[1].is_on").isEmpty());
        consumer.consume(snapshot(9).toString());
        mvc.perform(get("/api/monitoring/admin/selected-scene")
                .param("householdId", "r3-test").param("runId", "run-1").param("profileId", "kettle-profile"))
                .andExpect(jsonPath("$.source_index").value(10));
        mvc.perform(get("/api/monitoring/admin/selected-scene")
                .param("householdId", "r3-test").param("runId", "other-run").param("profileId", "kettle-profile"))
                .andExpect(status().isNotFound());
    }

    @Test void rejectsInventedOffAndConflictingRetry() throws Exception {
        ObjectNode node = snapshot(10);
        consumer.consume(node.toString());
        ((ObjectNode) node.path("appliances").get(0)).put("probability", .8);
        assertThatThrownBy(() -> consumer.consume(node.toString())).isInstanceOf(IllegalArgumentException.class);
        ObjectNode invalid = snapshot(11);
        ((ObjectNode) invalid.path("appliances").get(1)).put("state", "OFF").put("is_on", false);
        assertThatThrownBy(() -> consumer.consume(invalid.toString())).isInstanceOf(IllegalArgumentException.class);
        assertThat(repository.count()).isEqualTo(1);
    }

    @Test void readsActualPythonCheckpointOutputThroughDatabaseAndApi() throws Exception {
        String file = System.getenv("R3_EVIDENCE_FILE");
        assumeTrue(file != null, "Set R3_EVIDENCE_FILE to actual Python scene output JSONL");
        var lines = Files.readAllLines(Path.of(file));
        for (String line : lines) consumer.consume(line);
        assertThat(repository.count()).isEqualTo(592);
        var last = mapper.readTree(lines.getLast());
        mvc.perform(get("/api/monitoring/admin/selected-scene")
                .param("householdId", last.path("household_id").asText())
                .param("runId", last.path("run_id").asText())
                .param("profileId", last.path("profile_id").asText()))
                .andExpect(status().isOk()).andExpect(content().json(last.toString()));
    }
}

package com.nilm.monitoring.scene;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.scene-demo.enabled", havingValue = "true")
public class SceneSnapshotService {
    private static final List<String> ORDER = List.of(
            "KETTLE", "INDUCTION", "IRON", "MICROWAVE", "HAIR_DRYER", "VACUUM_CLEANER");
    private final SceneSnapshotRepository repository;
    private final ObjectMapper mapper;

    @Transactional
    public void accept(String body) throws JsonProcessingException {
        JsonNode node = mapper.readTree(body);
        validate(node);
        String id = node.get("snapshot_id").asText();
        Optional<SceneSnapshot> existing = repository.findById(id);
        if (existing.isPresent()) {
            if (!mapper.readTree(existing.get().getPayload()).equals(node)) {
                throw new IllegalArgumentException("Conflicting scene snapshot retry");
            }
            return;
        }
        repository.saveAndFlush(new SceneSnapshot(id, node.get("household_id").asText(),
                node.get("run_id").asText(), node.get("profile_id").asText(),
                node.get("source_index").asLong(), body));
    }

    @Transactional(readOnly = true)
    public Optional<JsonNode> read(String householdId, String runId, String profileId, Long sourceIndex) {
        Optional<SceneSnapshot> row = sourceIndex == null
                ? repository.findTopByHouseholdIdAndRunIdAndProfileIdOrderBySourceIndexDesc(householdId, runId, profileId)
                : repository.findByHouseholdIdAndRunIdAndProfileIdAndSourceIndex(householdId, runId, profileId, sourceIndex);
        return row.map(value -> {
            try { return mapper.readTree(value.getPayload()); }
            catch (JsonProcessingException error) { throw new IllegalStateException("Invalid stored scene JSON", error); }
        });
    }

    static void validate(JsonNode node) {
        if (node == null || node.path("schema_version").asInt() != 2
                || !"selected_scene".equals(node.path("scope").asText())) {
            throw new IllegalArgumentException("Expected selected-scene v2 snapshot");
        }
        text(node, "household_id", 50);
        text(node, "run_id", 100);
        text(node, "profile_id", 100);
        UUID.fromString(text(node, "snapshot_id", 36));
        OffsetDateTime.parse(text(node, "observed_at", 50));
        OffsetDateTime.parse(text(node, "published_at", 50));
        if (!node.path("source_index").isIntegralNumber() || !node.path("source_index").canConvertToLong()
                || node.path("source_index").asLong() < 0 || !node.path("ready").isBoolean()) {
            throw new IllegalArgumentException("Invalid scene position/readiness");
        }
        String target = text(node, "target_appliance", 30);
        JsonNode items = node.path("appliances");
        if (!ORDER.contains(target) || !items.isArray() || items.size() != ORDER.size()) {
            throw new IllegalArgumentException("Expected six ordered scene appliance entries");
        }
        for (int i = 0; i < ORDER.size(); i++) {
            JsonNode item = items.get(i);
            String name = item.path("appliance_type").asText();
            String state = item.path("state").asText();
            if (!ORDER.get(i).equals(name) || !item.path("inferred").isBoolean()
                    || !List.of("UNKNOWN", "ON", "OFF").contains(state)) {
                throw new IllegalArgumentException("Invalid scene state");
            }
            boolean inferred = item.get("inferred").asBoolean();
            if (inferred != (name.equals(target) && node.get("ready").asBoolean())) {
                throw new IllegalArgumentException("Inference scope mismatch");
            }
            if (!inferred) {
                if (!"UNKNOWN".equals(state) || !item.path("probability").isNull() || !item.path("is_on").isNull()) {
                    throw new IllegalArgumentException("Not-inferred appliance must remain UNKNOWN/null");
                }
            } else {
                JsonNode score = item.path("probability");
                if (!score.isNumber() || !Double.isFinite(score.asDouble()) || score.asDouble() < 0 || score.asDouble() > 1) {
                    throw new IllegalArgumentException("Invalid scene probability");
                }
                if ("UNKNOWN".equals(state) ? !item.path("is_on").isNull()
                        : !item.path("is_on").isBoolean() || item.get("is_on").asBoolean() != "ON".equals(state)) {
                    throw new IllegalArgumentException("State/is_on mismatch");
                }
            }
        }
    }

    private static String text(JsonNode node, String field, int limit) {
        JsonNode value = node.path(field);
        if (!value.isTextual() || value.asText().isBlank() || value.asText().length() > limit) {
            throw new IllegalArgumentException("Invalid scene field: " + field);
        }
        return value.asText();
    }
}

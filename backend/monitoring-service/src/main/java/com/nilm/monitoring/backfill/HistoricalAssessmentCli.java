package com.nilm.monitoring.backfill;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.nilm.monitoring.config.RiskProperties;
import com.nilm.monitoring.dto.kafka.*;
import com.nilm.monitoring.risk.RiskAssessor;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.*;
import java.util.*;

/** Standalone file-to-file batch. Does not start Spring, Kafka, a database or schedulers. */
public final class HistoricalAssessmentCli {
    static final ObjectMapper JSON = new ObjectMapper().findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES);
    public record AwayPeriod(OffsetDateTime start, OffsetDateTime end) {}
    public record Config(String householdId, OffsetDateTime start, OffsetDateTime end,
                         long stepSeconds, String snapshots, String profiles, String policy,
                         List<AwayPeriod> awayPeriods) {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("Usage: historicalAssessment <config.json> <new-output-directory>");
        run(Path.of(args[0]), Path.of(args[1]));
    }

    public static void run(Path configPath, Path output) throws Exception {
        configPath = configPath.toAbsolutePath();
        Config config = JSON.readValue(configPath.toFile(), Config.class);
        if (config.householdId() == null || config.householdId().isBlank() || config.start() == null
                || config.end() == null || !config.start().isBefore(config.end()) || config.stepSeconds() < 1
                || config.awayPeriods() == null) throw new IllegalArgumentException("invalid config; awayPeriods must be explicit");
        for (var away : config.awayPeriods()) {
            if (away.start() == null || away.end() == null || !away.start().isBefore(away.end()))
                throw new IllegalArgumentException("invalid away interval");
        }
        Path root = configPath.getParent();
        Path snapshots = root.resolve(config.snapshots());
        Path profilesPath = root.resolve(config.profiles());
        Path policyPath = root.resolve(config.policy());
        var properties = JSON.readValue(policyPath.toFile(), RiskProperties.class);
        var policy = properties.toPolicy();
        var profiles = new HistoricalProfileResolver(JSON.readValue(profilesPath.toFile(),
                new TypeReference<List<HouseholdProfileMessage>>() {}));
        var state = new HistoricalStateProvider(config.householdId(), properties.getObservationGapThreshold());
        var assessor = new RiskAssessor();
        var inputHashes = new LinkedHashMap<String, String>();
        inputHashes.put("config", digest(configPath));
        inputHashes.put("snapshots", digest(snapshots));
        inputHashes.put("profiles", digest(profilesPath));
        inputHashes.put("policy", digest(policyPath));
        String runId = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(
                JSON.writeValueAsBytes(List.of("historical-assessment-v1", RiskAssessor.SCORE_VERSION, inputHashes))));
        // Existing runs are immutable. Identical inputs in another directory produce identical IDs.
        Files.createDirectory(output);
        long count = 0;
        try (var input = Files.newBufferedReader(snapshots);
             var writer = Files.newBufferedWriter(output.resolve("assessments.pending"))) {
            AnalysisSnapshotMessage next = read(input);
            for (var at = config.start(); at.isBefore(config.end()); at = at.plusSeconds(config.stepSeconds())) {
                while (next != null && !next.observedAt().isAfter(at)) {
                    state.accept(next);
                    next = read(input);
                }
                final var time = at;
                boolean away = config.awayPeriods().stream().anyMatch(p -> !time.isBefore(p.start()) && time.isBefore(p.end()));
                var current = state.at(at, away);
                var result = assessor.assess(profiles.resolve(config.householdId(), at, policy.profileMaxAge()), current, policy);
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("assessment_id", UUID.nameUUIDFromBytes((runId + ":" + at.toInstant()).getBytes(StandardCharsets.UTF_8)).toString());
                row.put("household_id", config.householdId());
                row.put("assessed_at", at.toInstant().toString());
                row.put("last_observed_at", current.lastObservedAt());
                row.put("assessment_status", result.status());
                row.put("risk_score", result.score());
                row.put("risk_level", result.level());
                row.put("confidence", result.confidence());
                row.put("profile_version", result.profileVersion());
                row.put("policy_version", result.policyVersion());
                row.put("score_version", result.scoreVersion());
                row.put("indicators", JSON.writeValueAsString(result.indicators()));
                row.put("backfill_run_id", runId);
                row.put("assessment_mode", "EVENT_TIME_REASSESSMENT");
                writer.write(JSON.writeValueAsString(row));
                writer.newLine();
                count++;
            }
            // Validate the complete stream, including its tail, before marking delivery complete.
            while (next != null) { state.accept(next); next = read(input); }
        }
        if (!inputHashes.get("snapshots").equals(digest(snapshots))
                || !inputHashes.get("profiles").equals(digest(profilesPath))
                || !inputHashes.get("policy").equals(digest(policyPath))
                || !inputHashes.get("config").equals(digest(configPath))) {
            throw new IllegalStateException("input changed during backfill");
        }
        Files.move(output.resolve("assessments.pending"), output.resolve("assessments.jsonl"));
        Map<String, Object> manifest = new LinkedHashMap<>();
        manifest.put("schema_version", 1);
        manifest.put("backfill_run_id", runId);
        manifest.put("household_id", config.householdId());
        manifest.put("assessment_cutoff", config.end());
        manifest.put("assessment_delivery_complete", true);
        manifest.put("record_count", count);
        manifest.put("input_hashes", inputHashes);
        manifest.put("resolved_policy", policy);
        manifest.put("assessment_mode", "EVENT_TIME_REASSESSMENT");
        manifest.put("created_at", Instant.now());
        manifest.put("data_sha256", digest(output.resolve("assessments.jsonl")));
        JSON.writerWithDefaultPrettyPrinter().writeValue(output.resolve("manifest.pending").toFile(), manifest);
        Files.move(output.resolve("manifest.pending"), output.resolve("manifest.json"));
    }

    private static AnalysisSnapshotMessage read(BufferedReader input) throws IOException {
        String line = input.readLine();
        if (line == null) return null;
        var snapshot = JSON.readValue(line, AnalysisSnapshotMessage.class);
        if (snapshot == null || snapshot.observedAt() == null) throw new IllegalArgumentException("snapshot missing observed_at");
        return snapshot;
    }

    public static String digest(Path path) throws Exception {
        var digest = MessageDigest.getInstance("SHA-256");
        try (var input = Files.newInputStream(path)) {
            byte[] buffer = new byte[65536];
            int n;
            while ((n = input.read(buffer)) != -1) digest.update(buffer, 0, n);
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}

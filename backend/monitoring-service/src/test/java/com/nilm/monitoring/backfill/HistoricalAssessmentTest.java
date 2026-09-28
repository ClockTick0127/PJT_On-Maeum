package com.nilm.monitoring.backfill;

import static org.assertj.core.api.Assertions.*;
import com.nilm.monitoring.dto.kafka.*;
import com.nilm.monitoring.config.RiskProperties;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HistoricalAssessmentTest {
    @TempDir Path directory;
    static final OffsetDateTime START = OffsetDateTime.parse("2026-09-20T00:00:00+09:00");
    static AnalysisSnapshotMessage snapshot(long second, boolean on) {
        return new AnalysisSnapshotMessage(1, "s" + second, "H001", START.plusSeconds(second),
                START.plusSeconds(second), null, List.of(new AnalysisSnapshotMessage.Appliance("IRON", on)));
    }

    @Test void futureUseIsNotKnownAndShortUseBecomesValidOnlyWhenObserved() {
        var state = new HistoricalStateProvider("H001", Duration.ofSeconds(120));
        state.accept(snapshot(0, false));
        state.accept(snapshot(1, true));
        state.accept(snapshot(9, true));
        assertThat(state.at(START.plusSeconds(9), false).activity().uses()).isEmpty();
        state.accept(snapshot(11, true));
        assertThat(state.at(START.plusSeconds(11), false).activity().uses()).hasSize(1);
        assertThat(state.at(START.plusSeconds(11), false).activity().uses().getFirst().endedAt()).isNull();
        assertThatThrownBy(() -> state.at(START, false)).isInstanceOf(IllegalArgumentException.class);
        state.accept(snapshot(20, false));
        state.accept(snapshot(30, true));
        state.accept(snapshot(40, false));
        assertThat(state.at(START.plusSeconds(40), false).activity().uses()).hasSize(1);
    }

    @Test void gapIsNotCountedAndInitialOnHasImputedStart() {
        var state = new HistoricalStateProvider("H001", Duration.ofSeconds(2));
        state.accept(snapshot(0, true));
        state.accept(snapshot(1, true));
        state.accept(snapshot(20, true));
        var result = state.at(START.plusSeconds(20), false);
        assertThat(result.observation().coveredSeconds(START.toLocalDate()).orElseThrow()).isEqualTo(1);
        assertThat(result.observation().continuousSince()).isEqualTo(START.plusSeconds(20));
        assertThat(result.activity().uses().getFirst().startImputed()).isTrue();
        state.accept(snapshot(20, true));
        assertThatThrownBy(() -> state.accept(snapshot(20, false))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> state.accept(snapshot(19, true))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void midnightCoverageUsesBusinessTimezone() {
        var state = new HistoricalStateProvider("H001", Duration.ofSeconds(120));
        state.accept(snapshot(-1, false));
        state.accept(snapshot(1, false));
        var result = state.at(START.plusSeconds(1), false);
        assertThat(result.observation().coveredSeconds(START.toLocalDate()).orElseThrow()).isEqualTo(1);
        assertThat(result.observation().coveredSeconds(START.toLocalDate().minusDays(1)).orElseThrow()).isEqualTo(1);
    }

    static HouseholdProfileMessage profile(String version, OffsetDateTime published, String mode, String quality) {
        return new HouseholdProfileMessage(1, "H001", version, 1L, mode, START.toLocalDate().minusDays(1),
                START.toLocalDate().minusDays(28), START.toLocalDate().minusDays(1), START, published,
                "snapshot", "rules", "stats", quality, List.of(), List.of());
    }

    @Test void profileMustBeAvailableReadyActiveAndNotStale() {
        var resolver = new HistoricalProfileResolver(List.of(profile("p", START.plusHours(6), "ACTIVE", "READY")));
        assertThat(resolver.resolve("H001", START, Duration.ofDays(3))).isEmpty();
        assertThat(resolver.resolve("H001", START.plusHours(6), Duration.ofDays(3))).isPresent();
        assertThat(resolver.resolve("H001", START.plusDays(4), Duration.ofDays(3)).orElseThrow().stale()).isTrue();
        for (var p : List.of(profile("s", START, "SHADOW", "READY"), profile("i", START, "ACTIVE", "INPUT_INCOMPLETE"))) {
            assertThat(new HistoricalProfileResolver(List.of(p)).resolve("H001", START, Duration.ofDays(3))).isEmpty();
        }
    }

    @Test void publishedSchemaVersionTwoIsAccepted() {
        var v2 = new HouseholdProfileMessage(2, "H001", "v2", 1L, "ACTIVE", START.toLocalDate().minusDays(1),
                START.toLocalDate().minusDays(28), START.toLocalDate().minusDays(1), START, START,
                "snapshot", "rules", "stats", "READY", List.of(), List.of());
        assertThat(new HistoricalProfileResolver(List.of(v2)).resolve("H001", START, Duration.ofDays(3))).isPresent();
        var v3 = new HouseholdProfileMessage(3, "H001", "v3", 1L, "ACTIVE", START.toLocalDate().minusDays(1),
                START.toLocalDate().minusDays(28), START.toLocalDate().minusDays(1), START, START,
                "snapshot", "rules", "stats", "READY", List.of(), List.of());
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> new HistoricalProfileResolver(List.of(v3)));
    }

    Path config() throws Exception {
        var json = HistoricalAssessmentCli.JSON;
        Files.writeString(directory.resolve("snapshots.jsonl"), json.writeValueAsString(snapshot(0, false)) + "\n"
                + json.writeValueAsString(snapshot(1, false)) + "\n");
        Files.writeString(directory.resolve("profiles.json"), "[]");
        Files.writeString(directory.resolve("policy.json"), "{}");
        Path config = directory.resolve("config.json");
        json.writeValue(config.toFile(), new HistoricalAssessmentCli.Config("H001", START, START.plusMinutes(2),
                60, "snapshots.jsonl", "profiles.json", "policy.json", List.of()));
        return config;
    }

    @Test void cliIsDeterministicAndPreservesUnavailableScore() throws Exception {
        Path config = config();
        HistoricalAssessmentCli.run(config, directory.resolve("one"));
        HistoricalAssessmentCli.run(config, directory.resolve("two"));
        assertThat(Files.readString(directory.resolve("one/assessments.jsonl")))
                .isEqualTo(Files.readString(directory.resolve("two/assessments.jsonl")));
        var rows = Files.readAllLines(directory.resolve("one/assessments.jsonl"));
        assertThat(rows).hasSize(2);
        var first = HistoricalAssessmentCli.JSON.readTree(rows.getFirst());
        assertThat(first.get("assessment_status").asText()).isEqualTo("LEARNING");
        assertThat(first.get("risk_score").isNull()).isTrue();
        assertThatThrownBy(() -> HistoricalAssessmentCli.run(config, directory.resolve("one")))
                .isInstanceOf(FileAlreadyExistsException.class);
    }

    @Test void invalidTailCannotPublishCompletedManifest() throws Exception {
        Path config = config();
        Files.writeString(directory.resolve("snapshots.jsonl"), "bad\n", StandardOpenOption.APPEND);
        assertThatThrownBy(() -> HistoricalAssessmentCli.run(config, directory.resolve("bad"))).isInstanceOf(Exception.class);
        assertThat(directory.resolve("bad/manifest.json")).doesNotExist();
    }

    @Test void realCalculatorProducesReferenceScoreWithoutLevelAndExcludesAway() throws Exception {
        var json = HistoricalAssessmentCli.JSON;
        // 평소 00:30까지 다섯 번 켜는 가구가 한 번도 켜지 않았다. z = 5, 점수 75다.
        var statistic = new HouseholdProfileMessage.Statistic("CUMULATIVE_ACTIVITY_START_COUNT", null,
                "ALL", "00:30", 20L, 20L, 5.0, 5.0, 0.0, "count", "READY");
        var profile = new HouseholdProfileMessage(1, "H001", "ready", 1L, "ACTIVE",
                START.toLocalDate().minusDays(1), START.toLocalDate().minusDays(28),
                START.toLocalDate().minusDays(1), START, START, "s", "r", "s", "READY",
                List.of(), List.of(statistic));
        json.writeValue(directory.resolve("profiles.json").toFile(), List.of(profile));
        json.writeValue(directory.resolve("policy.json").toFile(), new RiskProperties());
        // 자정부터 1분마다 봤다. 00:30 구간을 빠짐없이 봐야 활동 부족을 확정할 수 있다.
        var snapshots = new StringBuilder();
        for (long second = 0; second <= 32 * 60; second += 60) {
            snapshots.append(json.writeValueAsString(snapshot(second, false))).append('\n');
        }
        Files.writeString(directory.resolve("snapshots.jsonl"), snapshots.toString());
        Path config = directory.resolve("config.json");
        json.writeValue(config.toFile(), new HistoricalAssessmentCli.Config("H001", START.plusMinutes(30),
                START.plusMinutes(32), 60, "snapshots.jsonl", "profiles.json", "policy.json", List.of()));

        HistoricalAssessmentCli.run(config, directory.resolve("home"));
        var home = json.readTree(Files.readAllLines(directory.resolve("home/assessments.jsonl")).getFirst());
        assertThat(home.get("assessment_status").asText()).isEqualTo("VALID");
        assertThat(home.get("risk_score").asInt()).isEqualTo(75);
        // 자체 평가는 등급을 내지 않는다. 등급은 분석 서비스 이벤트가 정한다.
        assertThat(home.get("risk_level").isNull()).isTrue();
        assertThat(home.get("score_version").asText()).isEqualTo("monitoring-score-v2-A");

        json.writeValue(config.toFile(), new HistoricalAssessmentCli.Config("H001", START.plusMinutes(30),
                START.plusMinutes(32), 60, "snapshots.jsonl", "profiles.json", "policy.json",
                List.of(new HistoricalAssessmentCli.AwayPeriod(START, START.plusHours(1)))));
        HistoricalAssessmentCli.run(config, directory.resolve("away"));
        var away = json.readTree(Files.readAllLines(directory.resolve("away/assessments.jsonl")).getFirst());
        assertThat(away.get("risk_score").isNull()).isTrue();
    }
}

package com.nilm.monitoring;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nilm.monitoring.dto.kafka.HouseholdProfileMessage;
import com.nilm.monitoring.service.HouseholdProfileService;
import com.nilm.monitoring.service.ResolvedProfile;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Gold 배치가 실제로 만든 메시지를 이 서비스의 ObjectMapper와 수신 경로에 그대로 통과시킨다.
 *
 * <p>{@code contracts/gold-household-profile-v2.json}은 {@code gold_profile} 배치의 종단
 * 실행에서 아웃박스에 적힌 payload를 그대로 옮긴 것이다. 배치 쪽 테스트
 * ({@code tests/test_message_contract_fixture.py})는 생산자의 키 집합이 이 파일과 같음을
 * 확인하고, 여기서는 소비자가 그 파일을 읽어 운영 프로필로 반영할 수 있음을 확인한다.
 * 두 테스트가 같은 파일을 보므로 어느 쪽이 계약을 바꾸든 한쪽이 먼저 깨진다.
 */
@SpringBootTest
class GoldProfileMessageContractTest {

    private static final String FIXTURE = "/contracts/gold-household-profile-v2.json";

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    @Autowired HouseholdProfileService service;
    @Autowired ObjectMapper objectMapper;
    @Autowired JdbcTemplate jdbc;

    private JsonNode fixture;

    @BeforeEach
    void setup() throws IOException {
        jdbc.update("delete from household_routine_baselines");
        jdbc.update("delete from household_profile_statistics");
        jdbc.update("delete from household_profiles");
        jdbc.update("delete from notifications");
        jdbc.update("delete from analysis_events");
        jdbc.update("delete from subjects");
        jdbc.update("""
                insert into subjects(household_id, auth_sub, birth_date, name, phone, address)
                values ('H001', 'test-subject-3', DATE '1950-01-01', 'test', '010', 'test')
                """);
        try (InputStream in = getClass().getResourceAsStream(FIXTURE)) {
            assertThat(in).as("fixture %s", FIXTURE).isNotNull();
            fixture = objectMapper.readTree(in);
        }
    }

    /** 배치 실행에서 받은 가구 ID를 이 테스트의 등록 가구로 바꾼다. 나머지는 그대로 둔다. */
    private HouseholdProfileMessage message(String deliveryMode) throws IOException {
        ObjectNode node = fixture.deepCopy();
        node.put("household_id", "H001");
        node.put("delivery_mode", deliveryMode);
        return objectMapper.treeToValue(node, HouseholdProfileMessage.class);
    }

    @Test
    void theRealBatchPayloadDeserializesIntoTheConsumerContract() throws IOException {
        HouseholdProfileMessage message = message(fixture.get("delivery_mode").asText());

        assertThat(message.schemaVersion()).isEqualTo(2);
        assertThat(message.profileVersion()).isNotBlank();
        assertThat(message.profileRevision()).isGreaterThanOrEqualTo(1L);
        assertThat(message.asOfDate()).isEqualTo(LocalDate.parse(fixture.get("as_of_date").asText()));
        assertThat(message.windowStartDate()).isBefore(message.asOfDate());
        assertThat(message.windowEndDate()).isEqualTo(message.asOfDate());
        assertThat(message.effectiveFrom()).isNotNull();
        assertThat(message.publishedAt()).isNotNull();
        assertThat(message.inputSnapshotId()).isNotBlank();
        assertThat(message.qualityStatus()).isEqualTo("READY");

        assertThat(message.routineBaselines()).hasSize(fixture.get("routine_baselines").size());
        HouseholdProfileMessage.RoutineBaseline baseline = message.routineBaselines().get(0);
        assertThat(baseline.applianceType()).isNotBlank();
        assertThat(baseline.baselineScope()).isIn("OVERALL", "WEEKDAY");
        // 배치는 decimal을 문자열로 보낸다. 숫자로 강제 변환돼야 한다.
        assertThat(baseline.dailyUseProbability()).isNotNull();
        assertThat(baseline.reliabilityWeight()).isNotNull();
        assertThat(baseline.enabled()).isNotNull();

        assertThat(message.statistics()).hasSize(fixture.get("statistics").size());
        assertThat(message.statistics())
                .extracting(HouseholdProfileMessage.Statistic::metricName)
                .contains("CUMULATIVE_ACTIVITY_START_COUNT", "INACTIVITY_ELAPSED",
                        "LOGICAL_USE_ACTIVE_DURATION");
        // 시간대 구간은 평가가 찾는 표기(구간 끝 "HH:MM")여야 한다.
        assertThat(message.statistics())
                .filteredOn(row -> row.timeBucket() != null)
                .allSatisfy(row -> assertThat(row.timeBucket()).matches("\\d{2}:\\d{2}"));
    }

    @Test
    void theRealBatchPayloadBecomesTheOperationalProfileAndResolvesForEvaluation() throws IOException {
        HouseholdProfileMessage message = message("ACTIVE");

        service.receive(message);

        assertThat(jdbc.queryForObject(
                "select status from household_profiles where profile_version = ?",
                String.class, message.profileVersion())).isEqualTo("ACTIVE");
        assertThat(jdbc.queryForObject(
                "select count(*) from household_routine_baselines", Integer.class))
                .isEqualTo(message.routineBaselines().size());
        assertThat(jdbc.queryForObject(
                "select count(*) from household_profile_statistics", Integer.class))
                .isEqualTo(message.statistics().size());

        // 배치는 effective_from에 실행 시각을 적고, 소비자는 그 시각 전에는 프로필을 쓰지
        // 않는다. 캡처된 payload의 실행 시각은 as_of_date 며칠 뒤이므로 그 뒤에서 평가한다.
        OffsetDateTime evaluation = message.effectiveFrom().plusHours(1);
        LocalDate evaluationDate = evaluation.atZoneSameInstant(KST).toLocalDate();
        assertThat(evaluationDate).isAfter(message.asOfDate());
        Optional<ResolvedProfile> resolved = service.resolveActive("H001", evaluation);
        assertThat(resolved).isPresent();
        assertThat(resolved.get().profileVersion()).isEqualTo(message.profileVersion());
        // 유효기간(기본 3일)은 as_of_date 기준이다. 캡처 시점에 따라 stale일 수 있다.
        assertThat(resolved.get().stale())
                .isEqualTo(ChronoUnit.DAYS.between(message.asOfDate(), evaluationDate) > 3);
        assertThat(resolved.get().baselines()).hasSize(message.routineBaselines().size());
        // 무활동 지표가 찾는 키 조합이 그대로 남아 있다.
        HouseholdProfileMessage.Statistic inactivity = message.statistics().stream()
                .filter(row -> "INACTIVITY_ELAPSED".equals(row.metricName()))
                .findFirst()
                .orElseThrow();
        assertThat(resolved.get().statistic(
                inactivity.metricName(), inactivity.applianceType(),
                inactivity.weekdayGroup(), inactivity.timeBucket())).isPresent();
    }

    @Test
    void aShadowPayloadFromTheBatchIsStoredWithoutBecomingOperational() throws IOException {
        service.receive(message("SHADOW"));

        assertThat(jdbc.queryForObject(
                "select status from household_profiles", String.class)).isEqualTo("SHADOW");
        assertThat(service.resolveActive("H001", OffsetDateTime.now())).isEmpty();
    }
}

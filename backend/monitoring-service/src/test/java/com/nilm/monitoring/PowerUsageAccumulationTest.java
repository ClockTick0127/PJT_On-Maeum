package com.nilm.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import com.nilm.monitoring.dto.kafka.AnalysisSnapshotMessage;
import com.nilm.monitoring.service.ApplianceActivityService;
import com.nilm.monitoring.service.InMemoryLatestSnapshotStore;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 스냅샷의 순시 전력이 시간 구간 전력량으로 쌓이는 경로.
 * 소비자와 같은 길을 타도록 {@link ApplianceActivityService#handle}를 통해 넣는다.
 */
@SpringBootTest
class PowerUsageAccumulationTest {

    private static final List<String> APPLIANCE_TYPES = List.of(
            "KETTLE", "INDUCTION", "IRON", "MICROWAVE", "HAIR_DRYER", "VACUUM_CLEANER");

    @Autowired ApplianceActivityService service;
    @Autowired InMemoryLatestSnapshotStore snapshots;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void setup() {
        jdbc.update("delete from hourly_appliance_power_usage");
        jdbc.update("delete from hourly_power_usage");
        jdbc.update("delete from household_observations");
        jdbc.update("delete from notifications");
        jdbc.update("delete from risk_assessments");
        jdbc.update("delete from appliance_usage_episodes");
        jdbc.update("delete from appliance_states");
        jdbc.update("delete from subjects");
        jdbc.update("""
                insert into subjects(household_id, auth_sub, birth_date, name, phone, address)
                values ('H001', 'test-subject-3', DATE '1950-01-01', 'test', '010', 'test')
                """);
        snapshots.clear();
    }

    private AnalysisSnapshotMessage snapshot(
            String observedAt,
            String activePower,
            String... poweredOn
    ) {
        Set<String> on = Set.of(poweredOn);
        List<AnalysisSnapshotMessage.Appliance> appliances = new ArrayList<>();
        for (String type : APPLIANCE_TYPES) {
            appliances.add(new AnalysisSnapshotMessage.Appliance(type, on.contains(type)));
        }
        OffsetDateTime at = OffsetDateTime.parse(observedAt);
        return new AnalysisSnapshotMessage(
                1,
                UUID.randomUUID().toString(),
                "H001",
                at,
                at.plusNanos(125_000_000L),
                new AnalysisSnapshotMessage.Measurement(
                        activePower == null ? null : new BigDecimal(activePower),
                        null,
                        null,
                        null
                ),
                appliances
        );
    }

    private BigDecimal totalAt(String bucketStartAt) {
        return jdbc.queryForObject(
                "select energy_wh from hourly_power_usage where bucket_start_at = ?",
                BigDecimal.class,
                OffsetDateTime.parse(bucketStartAt));
    }

    private Integer observedSecondsAt(String bucketStartAt) {
        return jdbc.queryForObject(
                "select observed_seconds from hourly_power_usage where bucket_start_at = ?",
                Integer.class,
                OffsetDateTime.parse(bucketStartAt));
    }

    private int totalRowCount() {
        return jdbc.queryForObject(
                "select count(*) from hourly_power_usage", Integer.class);
    }

    private int applianceRowCount() {
        return jdbc.queryForObject(
                "select count(*) from hourly_appliance_power_usage", Integer.class);
    }

    private BigDecimal applianceAt(String bucketStartAt, String applianceType) {
        return jdbc.queryForObject("""
                        select energy_wh from hourly_appliance_power_usage
                        where bucket_start_at = ? and appliance_type = ?
                        """,
                BigDecimal.class,
                OffsetDateTime.parse(bucketStartAt),
                applianceType);
    }

    @Test
    void firstSnapshotOnlySetsTheIntegrationAnchor() {
        service.handle(snapshot("2026-09-17T01:00:00Z", "1200"));

        assertThat(totalRowCount()).isZero();
    }

    @Test
    void integratesBetweenConsecutiveSnapshots() {
        // 가전 전환이 하나도 없는 구간이다. 그래도 전력은 쌓여야 한다.
        service.handle(snapshot("2026-09-17T01:00:00Z", "1200"));
        service.handle(snapshot("2026-09-17T01:01:00Z", "1200"));

        // 1200W × 60초 = 20Wh
        assertThat(totalAt("2026-09-17T01:00:00Z")).isEqualByComparingTo("20");
    }

    @Test
    void averagesTheTwoEndpointsOfTheInterval() {
        service.handle(snapshot("2026-09-17T01:00:00Z", "600"));
        service.handle(snapshot("2026-09-17T01:01:00Z", "1800"));

        // (600 + 1800) / 2 × 60초 = 20Wh
        assertThat(totalAt("2026-09-17T01:00:00Z")).isEqualByComparingTo("20");
    }

    @Test
    void splitsEnergyAcrossAnHourBoundary() {
        service.handle(snapshot("2026-09-17T01:59:40Z", "1800"));
        service.handle(snapshot("2026-09-17T02:00:20Z", "1800"));

        // 1800W × 40초 = 20Wh를 20초씩 나눠 갖는다
        assertThat(totalAt("2026-09-17T01:00:00Z")).isEqualByComparingTo("10");
        assertThat(totalAt("2026-09-17T02:00:00Z")).isEqualByComparingTo("10");
    }

    @Test
    void skipsIntegrationAcrossADataGap() {
        service.handle(snapshot("2026-09-17T01:00:00Z", "1200"));
        service.handle(snapshot("2026-09-17T01:03:00Z", "1200"));

        // 180초는 공백 기준 120초를 넘는다. 없던 전력량을 만들지 않는다.
        assertThat(totalRowCount()).isZero();
    }

    @Test
    void resentSnapshotDoesNotAddTwice() {
        AnalysisSnapshotMessage second = snapshot("2026-09-17T01:01:00Z", "1200");

        service.handle(snapshot("2026-09-17T01:00:00Z", "1200"));
        service.handle(second);
        service.handle(second);

        assertThat(totalAt("2026-09-17T01:00:00Z")).isEqualByComparingTo("20");
    }

    @Test
    void doesNotIntegrateWithoutActivePower() {
        service.handle(snapshot("2026-09-17T01:00:00Z", null));
        service.handle(snapshot("2026-09-17T01:01:00Z", null));

        assertThat(totalRowCount()).isZero();
    }

    @Test
    void recordsHowMuchOfTheBucketWasObserved() {
        service.handle(snapshot("2026-09-17T01:00:00Z", "1200"));
        service.handle(snapshot("2026-09-17T01:01:00Z", "1200"));

        // 이 값이 있어야 "조용했다"와 "못 봤다"를 가를 수 있다
        assertThat(observedSecondsAt("2026-09-17T01:00:00Z")).isEqualTo(60);
    }

    @Test
    void splitsObservedSecondsAcrossAnHourBoundary() {
        service.handle(snapshot("2026-09-17T01:59:40Z", "1800"));
        service.handle(snapshot("2026-09-17T02:00:20Z", "1800"));

        assertThat(observedSecondsAt("2026-09-17T01:00:00Z")).isEqualTo(20);
        assertThat(observedSecondsAt("2026-09-17T02:00:00Z")).isEqualTo(20);
    }

    @Test
    void keepsObservedSecondsEvenWhenNoPowerWasUsed() {
        // 전력량이 0으로 반올림돼도 본 시간은 남아야 한다
        service.handle(snapshot("2026-09-17T01:00:00Z", "0"));
        service.handle(snapshot("2026-09-17T01:01:00Z", "0"));

        assertThat(totalAt("2026-09-17T01:00:00Z")).isEqualByComparingTo("0");
        assertThat(observedSecondsAt("2026-09-17T01:00:00Z")).isEqualTo(60);
    }

    @Test
    void allocatesRemainderToPoweredOnAppliancesByWeight() {
        service.handle(snapshot("2026-09-17T01:00:00Z", "1082", "KETTLE", "MICROWAVE"));
        service.handle(snapshot("2026-09-17T01:01:00Z", "1082", "KETTLE", "MICROWAVE"));

        // 1082W × 60초 = 18.033333Wh, 기저부하 82W × 60초 = 1.366667Wh
        BigDecimal total = totalAt("2026-09-17T01:00:00Z");
        assertThat(total).isEqualByComparingTo("18.033333");

        BigDecimal kettle = applianceAt("2026-09-17T01:00:00Z", "KETTLE");
        BigDecimal microwave = applianceAt("2026-09-17T01:00:00Z", "MICROWAVE");

        // 남은 16.666667Wh를 1657:941로 나눈다
        assertThat(kettle.doubleValue()).isCloseTo(10.62997, within(0.001));
        assertThat(microwave.doubleValue()).isCloseTo(6.03669, within(0.001));

        // 배분이므로 가전별 합은 합산을 넘지 않고, 차이가 기저부하다
        assertThat(kettle.add(microwave).doubleValue())
                .isCloseTo(total.doubleValue() - 1.366667, within(0.001));
    }

    @Test
    void poweredOffAppliancesGetNothing() {
        service.handle(snapshot("2026-09-17T01:00:00Z", "1082", "KETTLE"));
        service.handle(snapshot("2026-09-17T01:01:00Z", "1082", "KETTLE"));

        assertThat(applianceRowCount()).isEqualTo(1);
        assertThat(applianceAt("2026-09-17T01:00:00Z", "KETTLE")).isNotNull();
    }

    @Test
    void skipsApplianceAllocationBelowBaseLoad() {
        // 측정값이 기저부하에 못 미친다. 합산은 남기되 가전 몫은 만들지 않는다.
        service.handle(snapshot("2026-09-17T01:00:00Z", "50", "KETTLE"));
        service.handle(snapshot("2026-09-17T01:01:00Z", "50", "KETTLE"));

        assertThat(totalAt("2026-09-17T01:00:00Z")).isPositive();
        assertThat(applianceRowCount()).isZero();
    }
}

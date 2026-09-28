package com.nilm.monitoring;

import static org.assertj.core.api.Assertions.assertThat;

import com.nilm.monitoring.config.enums.StateChangeTrigger;
import com.nilm.monitoring.dto.kafka.AnalysisSnapshotMessage;
import com.nilm.monitoring.service.ApplianceActivityService;
import com.nilm.monitoring.service.SubjectStateChanged;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.event.ApplicationEvents;
import org.springframework.test.context.event.RecordApplicationEvents;

/**
 * 스냅샷 전환을 Gold와 같은 "유효 사용" 단위로 묶는 흐름.
 *
 * <p>사용 횟수와 당일 사용 여부는 서로 다른 질문이다. 자정을 넘겨 이어진 사용은 새 날의
 * 시작이 아니지만 그 날 썼다는 사실은 참이다. 두 값이 같은 표에서 갈라져 나온다.
 */
@SpringBootTest
@RecordApplicationEvents
class ApplianceActivityFlowTest {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    @Autowired ApplianceActivityService service;
    @Autowired JdbcTemplate jdbc;
    @Autowired ApplicationEvents applicationEvents;

    private OffsetDateTime baseTime;

    @BeforeEach
    void setup() {
        jdbc.update("delete from notifications");
        jdbc.update("delete from risk_assessments");
        jdbc.update("delete from analysis_events");
        jdbc.update("delete from household_daily_appliance_usage");
        jdbc.update("delete from appliance_usage_episodes");
        jdbc.update("delete from appliance_states");
        jdbc.update("delete from household_observations");
        jdbc.update("delete from subjects");
        jdbc.update("""
                insert into subjects(household_id, auth_sub, birth_date, name, phone, address)
                values ('H001', 'test-subject-3', DATE '1950-01-01', 'test', '010', 'test')
                """);
        baseTime = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS);
        applicationEvents.clear();
    }

    private AnalysisSnapshotMessage snapshot(
            String householdId,
            OffsetDateTime observedAt,
            String onAppliance
    ) {
        List<AnalysisSnapshotMessage.Appliance> appliances = List.of(
                new AnalysisSnapshotMessage.Appliance("KETTLE", "KETTLE".equals(onAppliance)),
                new AnalysisSnapshotMessage.Appliance("INDUCTION", "INDUCTION".equals(onAppliance)),
                new AnalysisSnapshotMessage.Appliance("IRON", "IRON".equals(onAppliance)),
                new AnalysisSnapshotMessage.Appliance("MICROWAVE", "MICROWAVE".equals(onAppliance)),
                new AnalysisSnapshotMessage.Appliance("HAIR_DRYER", "HAIR_DRYER".equals(onAppliance)),
                new AnalysisSnapshotMessage.Appliance("VACUUM_CLEANER",
                        "VACUUM_CLEANER".equals(onAppliance))
        );
        return new AnalysisSnapshotMessage(
                1,
                UUID.randomUUID().toString(),
                householdId,
                observedAt,
                observedAt.plusNanos(125_000_000L),
                new AnalysisSnapshotMessage.Measurement(null, null, null, null),
                appliances
        );
    }

    private OffsetDateTime lastActivityAt() {
        return jdbc.queryForObject(
                "select last_activity_at from subjects", OffsetDateTime.class);
    }

    private String lastActivityAppliance() {
        return jdbc.queryForObject(
                "select last_activity_appliance from subjects", String.class);
    }

    private int episodeCount(String applianceType) {
        return jdbc.queryForObject(
                "select count(*) from appliance_usage_episodes where appliance_type = ?",
                Integer.class, applianceType);
    }

    private boolean episodeValid(String applianceType) {
        return Boolean.TRUE.equals(jdbc.queryForObject(
                """
                select is_valid from appliance_usage_episodes
                where appliance_type = ? order by started_at desc limit 1
                """,
                Boolean.class, applianceType));
    }

    private long activeSeconds(String applianceType) {
        return jdbc.queryForObject(
                """
                select active_seconds from appliance_usage_episodes
                where appliance_type = ? order by started_at desc limit 1
                """,
                Long.class, applianceType);
    }

    private Integer startCount(String applianceType, LocalDate date) {
        List<Integer> rows = jdbc.queryForList(
                """
                select start_count from household_daily_appliance_usage
                where appliance_type = ? and usage_date = ?
                """,
                Integer.class, applianceType, date);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private OffsetDateTime firstOnAt(String applianceType, LocalDate date) {
        return jdbc.queryForObject(
                """
                select first_on_at from household_daily_appliance_usage
                where appliance_type = ? and usage_date = ?
                """,
                OffsetDateTime.class, applianceType, date);
    }

    private LocalDate businessDate(OffsetDateTime at) {
        return at.atZoneSameInstant(KST).toLocalDate();
    }

    @Test
    void firstSnapshotOnlySeedsStateWithoutRecordingActivity() {
        service.handle(snapshot("H001", baseTime, "MICROWAVE"));

        assertThat(jdbc.queryForObject(
                "select count(*) from appliance_states", Integer.class)).isEqualTo(6);
        assertThat(lastActivityAt()).isNull();
        assertThat(applicationEvents.stream(SubjectStateChanged.class).count()).isZero();
    }

    @Test
    void onToOffTransitionRecordsLastActivity() {
        service.handle(snapshot("H001", baseTime, "MICROWAVE"));
        service.handle(snapshot("H001", baseTime.plusMinutes(10), null));

        assertThat(lastActivityAppliance()).isEqualTo("MICROWAVE");
        assertThat(lastActivityAt()).isEqualTo(baseTime.plusMinutes(10));
        // 전환이 있었으므로 위험 평가도 같은 트랜잭션에서 함께 돈다.
        assertThat(applicationEvents.stream(SubjectStateChanged.class)
                .map(SubjectStateChanged::trigger))
                .contains(StateChangeTrigger.ACTIVITY);
    }

    @Test
    void offToOnTransitionDoesNotRecordActivity() {
        service.handle(snapshot("H001", baseTime, null));
        service.handle(snapshot("H001", baseTime.plusMinutes(10), "KETTLE"));

        assertThat(lastActivityAt()).isNull();
        assertThat(jdbc.queryForObject(
                "select is_on from appliance_states where appliance_type='KETTLE'",
                Boolean.class)).isTrue();
    }

    @Test
    void unchangedSnapshotKeepsPreviousActivity() {
        service.handle(snapshot("H001", baseTime, "KETTLE"));
        service.handle(snapshot("H001", baseTime.plusMinutes(10), null));
        applicationEvents.clear();

        service.handle(snapshot("H001", baseTime.plusMinutes(20), null));

        assertThat(lastActivityAt()).isEqualTo(baseTime.plusMinutes(10));
        assertThat(applicationEvents.stream(SubjectStateChanged.class).count()).isZero();
    }

    @Test
    void lateSnapshotDoesNotRollBackLastActivity() {
        service.handle(snapshot("H001", baseTime, "KETTLE"));
        service.handle(snapshot("H001", baseTime.plusMinutes(10), null));

        // 이미 반영한 전환보다 앞선 스냅샷이 뒤늦게 도착한다.
        service.handle(snapshot("H001", baseTime.plusMinutes(5), "IRON"));

        assertThat(lastActivityAppliance()).isEqualTo("KETTLE");
        assertThat(lastActivityAt()).isEqualTo(baseTime.plusMinutes(10));
        assertThat(jdbc.queryForObject(
                "select is_on from appliance_states where appliance_type='IRON'",
                Boolean.class)).isFalse();
        // 뒤늦게 도착한 스냅샷으로 없던 사용을 만들지 않는다.
        assertThat(episodeCount("IRON")).isZero();
    }

    @Test
    void replayedSnapshotIsIdempotent() {
        service.handle(snapshot("H001", baseTime, "KETTLE"));
        var off = snapshot("H001", baseTime.plusMinutes(10), null);
        service.handle(off);
        applicationEvents.clear();

        service.handle(off);

        assertThat(lastActivityAt()).isEqualTo(baseTime.plusMinutes(10));
        assertThat(applicationEvents.stream(SubjectStateChanged.class).count()).isZero();
    }

    @Test
    void snapshotOfUnknownHouseholdIsIgnored() {
        service.handle(snapshot("H999", baseTime, "KETTLE"));

        assertThat(jdbc.queryForObject(
                "select count(*) from appliance_states", Integer.class)).isZero();
    }

    @Test
    void 열초보다_짧은_사용은_유효_사용으로_세지_않는다() {
        service.handle(snapshot("H001", baseTime, null));
        service.handle(snapshot("H001", baseTime.plusSeconds(30), "KETTLE"));
        service.handle(snapshot("H001", baseTime.plusSeconds(39), null));

        // 사용 자체는 남기지만 Gold가 세지 않는 사용이다.
        assertThat(episodeCount("KETTLE")).isEqualTo(1);
        assertThat(episodeValid("KETTLE")).isFalse();
        assertThat(startCount("KETTLE", businessDate(baseTime))).isNull();
    }

    @Test
    void 정확히_열초인_사용은_유효하다() {
        service.handle(snapshot("H001", baseTime, null));
        service.handle(snapshot("H001", baseTime.plusSeconds(30), "KETTLE"));
        service.handle(snapshot("H001", baseTime.plusSeconds(40), null));

        assertThat(episodeValid("KETTLE")).isTrue();
        assertThat(activeSeconds("KETTLE")).isEqualTo(10);
        assertThat(startCount("KETTLE", businessDate(baseTime))).isEqualTo(1);
        assertThat(firstOnAt("KETTLE", businessDate(baseTime)))
                .isEqualTo(baseTime.plusSeconds(30));
    }

    @Test
    void 진행_중인_사용도_관측이_쌓이면_유효로_확정된다() {
        service.handle(snapshot("H001", baseTime, null));
        service.handle(snapshot("H001", baseTime.plusSeconds(30), "KETTLE"));
        assertThat(episodeValid("KETTLE")).isFalse();

        // 아직 켜져 있지만 관측만으로 최소 사용시간을 넘겼다.
        service.handle(snapshot("H001", baseTime.plusSeconds(45), "KETTLE"));

        assertThat(episodeValid("KETTLE")).isTrue();
        assertThat(startCount("KETTLE", businessDate(baseTime))).isEqualTo(1);
    }

    @Test
    void 병합_간격_안의_재시작은_한_번의_사용이다() {
        // 전기포트의 병합 간격은 60초다. 경계값은 병합 쪽에 든다.
        service.handle(snapshot("H001", baseTime, null));
        service.handle(snapshot("H001", baseTime.plusSeconds(10), "KETTLE"));
        service.handle(snapshot("H001", baseTime.plusSeconds(30), null));
        service.handle(snapshot("H001", baseTime.plusSeconds(90), "KETTLE"));
        service.handle(snapshot("H001", baseTime.plusSeconds(110), null));

        assertThat(episodeCount("KETTLE")).isEqualTo(1);
        // 사용시간은 켜져 있던 구간만 더한다. 사이의 60초는 빼고 40초다.
        assertThat(activeSeconds("KETTLE")).isEqualTo(40);
        assertThat(startCount("KETTLE", businessDate(baseTime))).isEqualTo(1);
    }

    @Test
    void 병합_간격을_넘긴_재시작은_새_사용이다() {
        service.handle(snapshot("H001", baseTime, null));
        service.handle(snapshot("H001", baseTime.plusSeconds(10), "KETTLE"));
        service.handle(snapshot("H001", baseTime.plusSeconds(30), null));
        // 61초 뒤. 병합 간격을 1초 넘겼다.
        service.handle(snapshot("H001", baseTime.plusSeconds(91), "KETTLE"));
        service.handle(snapshot("H001", baseTime.plusSeconds(111), null));

        assertThat(episodeCount("KETTLE")).isEqualTo(2);
        assertThat(startCount("KETTLE", businessDate(baseTime))).isEqualTo(2);
    }

    @Test
    void 병합_간격은_가전마다_다르다() {
        // 다리미는 300초까지 한 번의 사용으로 본다. 같은 간격이라도 전기포트는 두 번이다.
        service.handle(snapshot("H001", baseTime, null));
        service.handle(snapshot("H001", baseTime.plusSeconds(10), "IRON"));
        service.handle(snapshot("H001", baseTime.plusSeconds(40), null));
        service.handle(snapshot("H001", baseTime.plusSeconds(340), "IRON"));
        service.handle(snapshot("H001", baseTime.plusSeconds(370), null));

        assertThat(episodeCount("IRON")).isEqualTo(1);
        assertThat(startCount("IRON", businessDate(baseTime))).isEqualTo(1);
    }

    @Test
    void 처음_본_순간_이미_켜져_있던_사용은_시작으로_세지_않는다() {
        service.handle(snapshot("H001", baseTime, "MICROWAVE"));
        service.handle(snapshot("H001", baseTime.plusMinutes(5), null));

        assertThat(episodeCount("MICROWAVE")).isEqualTo(1);
        assertThat(episodeValid("MICROWAVE")).isTrue();
        assertThat(jdbc.queryForObject(
                "select start_imputed from appliance_usage_episodes where appliance_type='MICROWAVE'",
                Boolean.class)).isTrue();
        // 언제 켜졌는지 모르는 사용이라 시작 횟수에 넣지 않는다.
        // 그래도 그 날 썼다는 사실은 남는다.
        assertThat(startCount("MICROWAVE", businessDate(baseTime))).isZero();
    }

    @Test
    void 중복_스냅샷은_사용을_두_번_만들지_않는다() {
        service.handle(snapshot("H001", baseTime, null));
        var on = snapshot("H001", baseTime.plusSeconds(10), "KETTLE");
        service.handle(on);
        service.handle(on);
        service.handle(snapshot("H001", baseTime.plusSeconds(40), null));

        assertThat(episodeCount("KETTLE")).isEqualTo(1);
        assertThat(startCount("KETTLE", businessDate(baseTime))).isEqualTo(1);
    }

    @Test
    void 자정을_넘겨_이어진_사용은_새_날의_시작으로_세지_않는다() {
        OffsetDateTime midnight = LocalDate.of(2026, 5, 11).atStartOfDay(KST).toOffsetDateTime();
        OffsetDateTime before = midnight.minusMinutes(5);

        service.handle(snapshot("H001", before.minusMinutes(1), null));
        service.handle(snapshot("H001", before, "IRON"));
        // 자정 전에 이미 유효 사용으로 확정된다.
        service.handle(snapshot("H001", before.plusMinutes(1), "IRON"));
        // 자정을 넘긴 첫 관측. 아직 켜져 있다.
        service.handle(snapshot("H001", midnight.plusMinutes(1), "IRON"));
        service.handle(snapshot("H001", midnight.plusMinutes(20), null));

        LocalDate startDay = LocalDate.of(2026, 5, 10);
        LocalDate nextDay = LocalDate.of(2026, 5, 11);

        assertThat(episodeCount("IRON")).isEqualTo(1);
        // 시작은 시작한 날에만 든다.
        assertThat(startCount("IRON", startDay)).isEqualTo(1);
        assertThat(startCount("IRON", nextDay)).isZero();
        // 새 날에도 "썼다"는 사실은 남고, 그 날의 첫 사용 시각은 자정이다.
        assertThat(firstOnAt("IRON", nextDay)).isEqualTo(midnight);
        assertThat(firstOnAt("IRON", startDay)).isEqualTo(before);
    }
}

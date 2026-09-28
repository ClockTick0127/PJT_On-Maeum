package com.nilm.monitoring;

import static org.assertj.core.api.Assertions.assertThat;

import com.nilm.monitoring.config.enums.StateChangeTrigger;
import com.nilm.monitoring.dto.kafka.AnalysisEventMessage;
import com.nilm.monitoring.dto.kafka.AnalysisSnapshotMessage;
import com.nilm.monitoring.service.AnalysisEventService;
import com.nilm.monitoring.service.ApplianceActivityService;
import com.nilm.monitoring.service.RiskAssessmentService;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 모니터링 자체 평가가 참고 점수를 남기고, 등급은 이벤트 슬롯이 정하는 흐름.
 *
 * <p>평가 기준 시각을 KST 2026-09-21 12:10으로 고정하고 직접 넘긴다.
 */
@SpringBootTest
class RiskAssessmentFlowTest {

    private static final OffsetDateTime BASE = OffsetDateTime.parse("2026-09-21T12:10:00+09:00");

    /**
     * 비교 기준 시각의 구간. 프로필 통계는 구간 끝 시각에서 잰 값이라
     * 12:10에는 아직 오지 않은 "12:30"이 아니라 직전 경계인 "12:00"과 비교한다.
     * 12:30을 지나는 평가도 있어 두 구간을 같은 값으로 함께 심는다.
     */
    private static final String BUCKET = "12:00";

    private static final String NEXT_BUCKET = "12:30";

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    /** 평소 다섯 번 켜는데 오늘은 한 번 켰다. z = 4, 점수 50이다. */
    private static final int EXPECTED_SCORE = 50;

    @Autowired RiskAssessmentService assessments;
    @Autowired AnalysisEventService analysisEvents;
    @Autowired ApplianceActivityService activities;
    @Autowired JdbcTemplate jdbc;

    private Long subjectId;

    @BeforeEach
    void setup() {
        jdbc.update("delete from notifications");
        jdbc.update("delete from risk_assessments");
        jdbc.update("delete from analysis_events");
        jdbc.update("delete from appliance_usage_episodes");
        jdbc.update("delete from appliance_states");
        jdbc.update("delete from household_daily_appliance_usage");
        jdbc.update("delete from household_observations");
        jdbc.update("delete from household_profile_statistics");
        jdbc.update("delete from household_routine_baselines");
        jdbc.update("delete from household_profiles");
        jdbc.update("delete from subjects");
        jdbc.update("delete from notification_settings");
        jdbc.update("delete from managers");

        jdbc.update("""
                insert into managers(auth_sub, name, organization)
                values ('test-manager-1', '담당', '센터')
                """);
        Long managerId = jdbc.queryForObject("select id from managers", Long.class);
        jdbc.update("""
                insert into subjects(household_id, auth_sub, birth_date, name, phone, address,
                        manager_id)
                values ('house-risk', 'test-subject-3', DATE '1950-01-01', 'test', '010', 'test', ?)
                """, managerId);
        subjectId = jdbc.queryForObject("select id from subjects", Long.class);

        seedProfile();
    }

    /** 활동 감소 비교 통계만 담은 프로필. 평소 12:00·12:30까지 다섯 번 켠다. */
    private void seedProfile() {
        jdbc.update("""
                insert into household_profiles(household_id, profile_version, as_of_date,
                        window_start_date, window_end_date, effective_from, received_at,
                        quality_status, status)
                values ('house-risk', 'profile-1', DATE '2026-09-20', DATE '2026-08-24',
                        DATE '2026-09-20', ?, ?, 'READY', 'ACTIVE')
                """, BASE.minusHours(6), BASE.minusHours(6));
        Long profileId = jdbc.queryForObject(
                "select id from household_profiles", Long.class);

        for (String bucket : new String[]{BUCKET, NEXT_BUCKET}) {
            jdbc.update("""
                    insert into household_profile_statistics(profile_id, metric_name,
                            weekday_group, time_bucket, sample_count, eligible_day_count,
                            p50, p90, mad, unit, quality_status)
                    values (?, 'CUMULATIVE_ACTIVITY_START_COUNT', 'ALL', ?, 20, 20, 5, 5, 0,
                            'count', 'READY')
                    """, profileId, bucket);
        }
    }

    /**
     * 평가가 볼 현재 상태를 기준 시각에 맞춘다.
     *
     * <p>평가는 관측 커버리지와 유효 사용 원장을 읽는다({@code CurrentStateProvider}).
     * 하루의 시작부터 끊김 없이 본 가구로 심어야 보지 못한 구간이 활동 부족으로 둔갑하지 않는다.
     * 오늘 07:00에 전기포트를 한 번 켰다.
     */
    private void observe(OffsetDateTime now) {
        OffsetDateTime dayStart = now.atZoneSameInstant(KST)
                .toLocalDate().atStartOfDay(KST).toOffsetDateTime();

        jdbc.update("delete from household_observations");
        jdbc.update("""
                insert into household_observations(household_id, last_observed_at,
                        continuous_since, coverage_date, covered_seconds, updated_at)
                values ('house-risk', ?, ?, ?, ?, ?)
                """,
                now, dayStart, dayStart.toLocalDate(),
                Duration.between(dayStart, now).getSeconds(), now);

        OffsetDateTime startedAt = dayStart.plusHours(7);
        jdbc.update("delete from appliance_usage_episodes");
        jdbc.update("""
                insert into appliance_usage_episodes(household_id, appliance_type, started_at,
                        start_imputed, ended_at, observed_until, active_seconds, segment_count,
                        is_valid, business_date, updated_at)
                values ('house-risk', 'KETTLE', ?, false, ?, ?, 180, 1, true, ?, ?)
                """,
                startedAt, startedAt.plusMinutes(3), startedAt.plusMinutes(3),
                dayStart.toLocalDate(), now);
    }

    private void evaluateAt(OffsetDateTime now) {
        observe(now);
        assessments.evaluateSubject(subjectId, now, StateChangeTrigger.ASSESSMENT);
    }

    private String column(String name) {
        return jdbc.queryForObject("select cast(" + name + " as varchar) from subjects", String.class);
    }

    private int notificationCount() {
        return jdbc.queryForObject("select count(*) from notifications", Integer.class);
    }

    @Test
    void 자체_평가는_참고_점수만_남기고_등급도_알림도_만들지_않는다() {
        evaluateAt(BASE);
        // v1이라면 유지시간(10분)을 채워 등급이 올랐을 시각이다.
        evaluateAt(BASE.plusMinutes(11));

        assertThat(column("assessment_status")).isEqualTo("VALID");
        assertThat(column("assessed_risk_score")).isEqualTo(String.valueOf(EXPECTED_SCORE));
        assertThat(column("assessed_risk_level")).isNull();
        assertThat(column("pending_risk_level")).isNull();
        assertThat(column("current_risk_level")).isEqualTo("NORMAL");
        // 이벤트 등급이 없는 동안 화면 점수는 참고 점수다.
        assertThat(column("current_risk_score")).isEqualTo(String.valueOf(EXPECTED_SCORE));
        assertThat(notificationCount()).isZero();
    }

    @Test
    void 평가가_성립하지_않으면_기존_점수를_덮어쓰지_않는다() {
        evaluateAt(BASE);
        assertThat(column("assessed_risk_score")).isEqualTo(String.valueOf(EXPECTED_SCORE));

        // 비교할 프로필이 사라졌다. 학습 중으로 되돌아간다.
        jdbc.update("update household_profiles set status = 'SUPERSEDED'");
        evaluateAt(BASE.plusMinutes(20));

        assertThat(column("assessment_status")).isEqualTo("LEARNING");
        // 평가 불가를 0점으로 덮어쓰지 않는다.
        assertThat(column("assessed_risk_score")).isEqualTo(String.valueOf(EXPECTED_SCORE));
    }

    @Test
    void v1이_세워_둔_자체_평가_등급은_다음_평가가_지운다() {
        // v1 점수식이 남긴 등급과 후보. 판단 주체가 사라져 해제될 길이 없다.
        jdbc.update("""
                update subjects
                set assessed_risk_level = 'WARNING', assessed_risk_score = 80,
                    pending_risk_level = 'DANGER', pending_since = ?,
                    current_risk_level = 'WARNING', current_risk_score = 80
                """, BASE.minusMinutes(5));

        // 평가가 성립하지 않아도 지운다. 옛 등급은 점수와 달리 남길 근거가 없다.
        jdbc.update("update household_profiles set status = 'SUPERSEDED'");
        evaluateAt(BASE);

        assertThat(column("assessed_risk_level")).isNull();
        assertThat(column("pending_risk_level")).isNull();
        assertThat(column("current_risk_level")).isEqualTo("NORMAL");
        assertThat(notificationCount()).isZero();
    }

    @Test
    void 평가_이력에_정책과_근거가_함께_남는다() {
        evaluateAt(BASE);

        assertThat(jdbc.queryForObject(
                "select count(*) from risk_assessments", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select policy_version from risk_assessments", String.class))
                .isEqualTo("policy-v2-experimental");
        assertThat(jdbc.queryForObject(
                "select score_version from risk_assessments", String.class))
                .isEqualTo("monitoring-score-v2-A");
        assertThat(jdbc.queryForObject(
                "select profile_version from risk_assessments", String.class))
                .isEqualTo("profile-1");
        assertThat(jdbc.queryForObject(
                "select risk_score from risk_assessments", Integer.class))
                .isEqualTo(EXPECTED_SCORE);
        assertThat(jdbc.queryForObject(
                "select risk_level from risk_assessments", String.class)).isNull();
        assertThat(jdbc.queryForObject(
                "select indicators from risk_assessments", String.class))
                .contains("\"code\":\"A\"")
                .contains("\"bucket\":\"12:00\"")
                // 루틴 미사용·무활동은 분석 서비스가 판단한다. 이력에도 남기지 않는다.
                .doesNotContain("\"code\":\"M\"")
                .doesNotContain("\"code\":\"I\"");
    }

    @Test
    void 이벤트_등급이_유효_등급이_되고_가전이_꺼지면_참고_점수로_돌아간다() {
        evaluateAt(BASE);
        assertThat(column("current_risk_level")).isEqualTo("NORMAL");

        OffsetDateTime observedAt = OffsetDateTime.now(ZoneOffset.UTC);
        // 켜진 상태를 먼저 심어 둔다. 첫 스냅샷은 전환으로 보지 않는다.
        activities.handle(snapshot(observedAt, true));

        analysisEvents.handle(new AnalysisEventMessage(
                UUID.randomUUID(),
                "house-risk",
                null,
                observedAt,
                Map.of("appliance_type", "KETTLE", "allowed_duration_minutes", 60),
                "PROLONGED_APPLIANCE_USE",
                "KETTLE"
        ));

        assertThat(column("current_risk_level")).isEqualTo("DANGER");
        assertThat(column("event_risk_appliance")).isEqualTo("KETTLE");

        // 그 가전이 꺼지면 이벤트 등급은 근거를 잃는다.
        activities.handle(snapshot(observedAt.plusMinutes(5), false));

        assertThat(column("event_risk_level")).isNull();
        assertThat(column("current_risk_level")).isEqualTo("NORMAL");
        assertThat(column("current_risk_score")).isEqualTo(String.valueOf(EXPECTED_SCORE));
    }

    private AnalysisSnapshotMessage snapshot(OffsetDateTime observedAt, boolean kettleOn) {
        return new AnalysisSnapshotMessage(
                1,
                UUID.randomUUID().toString(),
                "house-risk",
                observedAt,
                observedAt,
                new AnalysisSnapshotMessage.Measurement(null, null, null, null),
                List.of(new AnalysisSnapshotMessage.Appliance("KETTLE", kettleOn))
        );
    }
}

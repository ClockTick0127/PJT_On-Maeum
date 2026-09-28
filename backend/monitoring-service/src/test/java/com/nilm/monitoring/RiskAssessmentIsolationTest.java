package com.nilm.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;

import com.nilm.monitoring.config.enums.StateChangeTrigger;
import com.nilm.monitoring.scheduler.RiskAssessmentScheduler;
import com.nilm.monitoring.service.HouseholdProfileService;
import com.nilm.monitoring.service.RiskAssessmentService;
import com.nilm.monitoring.service.RiskAssessmentService.EvaluationTarget;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

/**
 * 타이머 평가의 가구 단위 격리.
 *
 * <p>여기서 확인하는 것은 잠금의 동시성 동작이 아니라 잠금을 가구 범위로 잡기 때문에
 * 성립해야 하는 성질들이다. 한 가구가 실패해도 나머지가 돌아야 하고, 가구마다 열린
 * 트랜잭션이 각자 커밋·롤백되어야 하며, 타이머가 이벤트 슬롯을 건드리지 않아야 한다.
 *
 * <p>PostgreSQL의 행 잠금이 실제로 두 트랜잭션을 직렬화하는지는 이 테스트로는 알 수 없다.
 * 그것은 {@code RiskAssessmentLockPostgresTest}가 진짜 PostgreSQL에서 확인한다.
 */
@SpringBootTest
class RiskAssessmentIsolationTest {

    private static final OffsetDateTime NOW = OffsetDateTime.parse("2026-09-21T12:10:00+09:00");

    @Autowired RiskAssessmentService assessments;
    @Autowired JdbcTemplate jdbc;

    @MockitoSpyBean HouseholdProfileService profiles;

    /**
     * 순회 코드는 그대로 쓰되 {@code @Scheduled} 빈으로 등록하지는 않는다.
     * 등록하면 컨텍스트가 뜨는 순간 배경에서 한 바퀴 돌고, 테스트들이 같은 인메모리 DB를
     * 공유하므로 다른 테스트의 단정이 흔들린다.
     */
    private RiskAssessmentScheduler scheduler;

    @BeforeEach
    void setup() {
        // 이벤트 등급을 심은 시각(NOW)에 시계를 고정한다. 실제 시각을 쓰면 유지시간 6시간이
        // 지난 뒤 실행될 때 등급이 만료되어 결과가 실행 시점에 따라 갈린다.
        scheduler = new RiskAssessmentScheduler(
                assessments, Clock.fixed(NOW.toInstant(), ZoneOffset.UTC));

        jdbc.update("delete from notifications");
        jdbc.update("delete from risk_assessments");
        jdbc.update("delete from analysis_events");
        jdbc.update("delete from household_observations");
        jdbc.update("delete from subjects");

        insertSubject("house-a", "auth-a");
        insertSubject("house-b", "auth-b");
    }

    private void insertSubject(String householdId, String authSub) {
        jdbc.update("""
                insert into subjects(household_id, auth_sub, birth_date, name, phone, address)
                values (?, ?, DATE '1950-01-01', 'test', '010', 'test')
                """, householdId, authSub);
    }

    private Long subjectId(String householdId) {
        return jdbc.queryForObject(
                "select id from subjects where household_id = ?", Long.class, householdId);
    }

    private EvaluationTarget target(String householdId) {
        return new EvaluationTarget(subjectId(householdId), householdId);
    }

    private String column(String householdId, String column) {
        return jdbc.queryForObject(
                "select cast(" + column + " as varchar) from subjects where household_id = ?",
                String.class, householdId);
    }

    private int assessmentCount(String householdId) {
        return jdbc.queryForObject(
                "select count(*) from risk_assessments where household_id = ?",
                Integer.class, householdId);
    }

    private int notificationCount() {
        return jdbc.queryForObject("select count(*) from notifications", Integer.class);
    }

    /** 이벤트 경로가 세운 등급을 흉내낸다. 해제 조건은 아직 오지 않은 상태다. */
    private void seedEventRisk(String householdId, String level, int score) {
        seedEventRisk(householdId, level, score, NOW);
    }

    /**
     * 평가가 쓰는 시계에 맞춰 이벤트 등급을 세운다.
     *
     * <p>스케줄러는 벽시계로 평가하므로 고정 시각 {@link #NOW}로 세운 등급은 실제 시각이
     * 최대 유지시간을 지나면 해제된다. 스케줄러를 거치는 단정은 벽시계 기준으로 심어야 한다.
     */
    private void seedEventRisk(String householdId, String level, int score, OffsetDateTime setAt) {
        jdbc.update("""
                update subjects
                set event_risk_level = ?, event_risk_score = ?, event_risk_appliance = 'KETTLE',
                    event_risk_set_at = ?, current_risk_level = ?, current_risk_score = ?
                where household_id = ?
                """, level, score, setAt, level, score, householdId);
    }

    @Test
    void 타이머는_대상자를_읽기_전에_가구를_알_수_있도록_식별자만_순회한다() {
        List<EvaluationTarget> targets = assessments.targets();

        // 목록이 엔티티를 싣지 않아야 가구 트랜잭션의 첫 읽기가 곧 쓰기 잠금이 된다.
        assertThat(targets).containsExactly(target("house-a"), target("house-b"));
    }

    @Test
    void 한_가구의_평가_실패가_다른_가구의_평가를_막지_않는다() {
        doThrow(new IllegalStateException("프로필 조회 실패"))
                .when(profiles).resolveActive(eq("house-a"), any());

        assertThatCode(() -> scheduler.evaluateAll()).doesNotThrowAnyException();

        // 실패한 가구의 트랜잭션만 되돌아간다. 상태도 이력도 남지 않는다.
        assertThat(column("house-a", "assessment_status")).isNull();
        assertThat(assessmentCount("house-a")).isZero();

        // 나머지 가구는 그대로 평가된다.
        assertThat(column("house-b", "assessment_status")).isEqualTo("LEARNING");
        assertThat(assessmentCount("house-b")).isEqualTo(1);
    }

    @Test
    void 서로_다른_가구는_각자의_결과로_독립적으로_반영된다() {
        // 한쪽에만 이벤트 등급이 서 있다. 스케줄러는 벽시계로 평가하므로 그 시계로 세운다.
        seedEventRisk("house-a", "DANGER", 95, OffsetDateTime.now(ZoneOffset.UTC));

        scheduler.evaluateAll();

        assertThat(column("house-a", "current_risk_level")).isEqualTo("DANGER");
        assertThat(column("house-b", "current_risk_level")).isEqualTo("NORMAL");
        assertThat(assessmentCount("house-a")).isEqualTo(1);
        assertThat(assessmentCount("house-b")).isEqualTo(1);
    }

    @Test
    void 타이머_평가는_유지시간_안의_이벤트_등급을_지우지_않는다() {
        seedEventRisk("house-a", "DANGER", 95);

        assessments.evaluateSubject(target("house-a"), NOW, StateChangeTrigger.ASSESSMENT);

        // 자체 평가는 학습 중이라 등급을 세우지 못한다. 이벤트 슬롯이 그대로 유효 등급이다.
        assertThat(column("house-a", "event_risk_level")).isEqualTo("DANGER");
        assertThat(column("house-a", "current_risk_level")).isEqualTo("DANGER");
        assertThat(column("house-a", "assessed_risk_level")).isNull();
    }

    @Test
    void 같은_재발송_창_안에서는_두_번째_평가가_알림을_다시_만들지_않는다() {
        seedEventRisk("house-a", "WARNING", 75);
        EvaluationTarget target = target("house-a");

        assessments.evaluateSubject(target, NOW, StateChangeTrigger.ASSESSMENT);
        assertThat(notificationCount()).isEqualTo(1);

        // 재발송 간격은 1시간이다. 같은 등급이 이어지는 동안에는 다시 만들지 않는다.
        assessments.evaluateSubject(target, NOW.plusMinutes(1), StateChangeTrigger.ASSESSMENT);
        assertThat(notificationCount()).isEqualTo(1);
    }

    @Test
    void 대상자_id만_아는_호출부도_같은_잠금_경로를_탄다() {
        seedEventRisk("house-a", "WARNING", 75);

        assessments.evaluateSubject(
                subjectId("house-a"), NOW, StateChangeTrigger.ASSESSMENT);

        assertThat(notificationCount()).isEqualTo(1);
        assertThat(column("house-a", "last_alert_at")).isNotNull();
    }
}

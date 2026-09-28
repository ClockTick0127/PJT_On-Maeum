package com.nilm.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.nilm.monitoring.scheduler.RiskAssessmentScheduler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 타이머 평가는 다른 테스트에서 꺼 둔다. 꺼 둔 채로만 돌면 배선이 깨져도 아무도 모른다.
 * 여기서만 켜서 실제로 돌려 본다.
 */
@SpringBootTest(properties = {
        "app.risk.scheduler-enabled=true",
        // 테스트가 도는 동안 배경에서 돌지 않게 첫 실행과 간격을 모두 길게 둔다.
        // 초기 지연이 없으면 컨텍스트가 뜨는 순간 배경 실행이 시작되어, 다른 테스트가
        // 공유 H2에 남긴 대상자까지 평가하며 이 테스트의 행 수 단정과 경쟁한다.
        "app.risk.scheduler-initial-delay-ms=3600000",
        "app.risk.scheduler-interval-ms=3600000"
})
class RiskAssessmentSchedulerTest {

    @Autowired RiskAssessmentScheduler scheduler;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void setup() {
        jdbc.update("delete from notifications");
        jdbc.update("delete from risk_assessments");
        jdbc.update("delete from analysis_events");
        jdbc.update("delete from subjects");
        jdbc.update("""
                insert into subjects(household_id, auth_sub, birth_date, name, phone, address)
                values ('house-sched', 'test-subject-3', DATE '1950-01-01', 'test', '010', 'test')
                """);
    }

    @Test
    void 프로필이_없는_가구도_멈추지_않고_학습_중으로_남긴다() {
        assertThatCode(() -> scheduler.evaluateAll()).doesNotThrowAnyException();

        // 비교할 프로필이 없다. 0점·정상이 아니라 학습 중이다.
        assertThat(jdbc.queryForObject(
                "select assessment_status from subjects", String.class)).isEqualTo("LEARNING");
        assertThat(jdbc.queryForObject(
                "select assessed_risk_level from subjects", String.class)).isNull();
        assertThat(jdbc.queryForObject(
                "select current_risk_level from subjects", String.class)).isEqualTo("NORMAL");
        assertThat(jdbc.queryForObject(
                "select count(*) from risk_assessments", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select count(*) from notifications", Integer.class)).isZero();
    }

    @Test
    void 결과가_그대로면_평가_이력을_계속_쌓지_않는다() {
        scheduler.evaluateAll();
        scheduler.evaluateAll();

        // 기록 간격(30분)이 지나지 않았고 등급도 상태도 그대로다.
        assertThat(jdbc.queryForObject(
                "select count(*) from risk_assessments", Integer.class)).isEqualTo(1);
    }
}

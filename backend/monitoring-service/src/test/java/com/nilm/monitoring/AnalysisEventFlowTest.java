package com.nilm.monitoring;

import com.nilm.monitoring.dto.kafka.AnalysisEventMessage;
import com.nilm.monitoring.service.AnalysisEventService;
import com.nilm.monitoring.service.NotificationReady;
import com.nilm.monitoring.service.WebPushSender;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.context.event.ApplicationEvents;
import org.springframework.test.context.event.RecordApplicationEvents;
import org.springframework.test.util.ReflectionTestUtils;
import static org.assertj.core.api.Assertions.*;

/**
 * 분석 서비스 이벤트의 유형별 라우팅.
 *
 * <p>이벤트 계약에는 score가 없다. 등급은 수신 점수가 아니라 유형별 정책에서 나온다.
 */
@SpringBootTest
@RecordApplicationEvents
class AnalysisEventFlowTest {
    @Autowired AnalysisEventService service;
    @Autowired JdbcTemplate jdbc;
    @Autowired ApplicationEvents applicationEvents;
    @Autowired ApplicationContext context;
    @Autowired KafkaListenerEndpointRegistry listeners;

    @BeforeEach
    void setup() {
        jdbc.update("delete from notifications");
        jdbc.update("delete from risk_assessments");
        jdbc.update("delete from analysis_events");
        jdbc.update("delete from subjects");
        jdbc.update("""
                insert into subjects(household_id, auth_sub, birth_date, name, phone, address)
                values ('house-test', 'test-subject-3', DATE '1950-01-01', 'test', '010', 'test')
                """);
        // 외부 푸시 없이 커밋 후 발송 이벤트 발행 여부만 확인한다.
        ReflectionTestUtils.setField(service, "pushEnabled", true);
        applicationEvents.clear();
    }

    /** 분석 서비스가 실제로 보내는 모양. score 필드는 없다. */
    AnalysisEventMessage message(String eventType, String applianceType) {
        return new AnalysisEventMessage(UUID.randomUUID(), "house-test", null,
                OffsetDateTime.now(ZoneOffset.UTC), Map.of("normal_days", 24),
                eventType, applianceType);
    }

    @Test
    void registersKafkaListenerAndStartsWithoutVapidKeys() {
        // analysis.event.v1(이상 징후), analysis.snapshot.v1(가전 ON/OFF),
        // gold.household-profile.v1(생활 프로필), device.manager-registered.v1(담당자 가입)
        // 네 개를 구독한다.
        // 분석 이벤트·스냅샷·생활 프로필·담당자 가입·기기 접속
        assertThat(listeners.getListenerContainers()).hasSize(5);
        assertThat(context.getBeansOfType(WebPushSender.class)).isEmpty();
        assertThat(listeners.getListenerContainers()).allMatch(c -> !c.isRunning());
    }

    @Test
    void 점수가_없는_이벤트도_컨슈머를_멈추지_않는다() {
        // 계약에 없는 score를 필수로 검사하면 실제 이벤트가 오는 순간 컨테이너가 멈춘다.
        assertThatCode(() -> service.handle(message("ROUTINE_MISSED", "KETTLE")))
                .doesNotThrowAnyException();
        assertThat(jdbc.queryForObject("select count(*) from analysis_events", Integer.class))
                .isEqualTo(1);
    }

    @Test
    void 그림자_유형은_저장만_하고_등급도_알림도_만들지_않는다() {
        // 루틴 미사용은 판단에 쓰지 않는다. 저장만 하고 등급·알림을 만들지 않는다.
        service.handle(message("ROUTINE_MISSED", "KETTLE"));

        assertThat(jdbc.queryForObject("select risk_level from analysis_events", String.class))
                .isEqualTo("NORMAL");
        assertThat(jdbc.queryForObject("select count(*) from notifications", Integer.class))
                .isZero();
        assertThat(jdbc.queryForObject("select current_risk_level from subjects", String.class))
                .isEqualTo("NORMAL");
        assertThat(jdbc.queryForObject("select event_risk_level from subjects", String.class))
                .isNull();
    }

    @Test
    void 알_수_없는_유형은_근거로만_저장한다() {
        service.handle(message("ROUTINE_CHANGED", null));

        assertThat(jdbc.queryForObject("select count(*) from analysis_events", Integer.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from notifications", Integer.class))
                .isZero();
        assertThat(jdbc.queryForObject("select state_version from subjects", Long.class))
                .isEqualTo(1L);
    }

    @Test
    void 장시간_사용은_위험_등급으로_알림을_한_번만_만든다() {
        var event = message("PROLONGED_APPLIANCE_USE", "KETTLE");
        service.handle(event);
        service.handle(event);

        assertThat(jdbc.queryForObject("select count(*) from analysis_events", Integer.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject("select risk_level from analysis_events", String.class))
                .isEqualTo("DANGER");
        // 계약에 score가 없으므로 저장 점수는 등급의 대표값이다.
        assertThat(jdbc.queryForObject("select risk_score from analysis_events", Integer.class))
                .isEqualTo(90);
        assertThat(jdbc.queryForObject("select count(*) from notifications", Integer.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject("select auth_sub from notifications", String.class))
                .isEqualTo("test-subject-3");
        assertThat(jdbc.queryForObject("select response_status from notifications", String.class))
                .isEqualTo("PENDING");
        assertThat(applicationEvents.stream(NotificationReady.class).findFirst().orElseThrow().body())
                .endsWith(NotificationReady.RESPONSE_QUESTION);
        assertThat(jdbc.queryForObject("select current_risk_level from subjects", String.class))
                .isEqualTo("DANGER");
        assertThat(jdbc.queryForObject("select event_risk_appliance from subjects", String.class))
                .isEqualTo("KETTLE");
        assertThat(applicationEvents.stream(NotificationReady.class).count()).isEqualTo(1);
    }

    @Test
    void 무활동은_주의_등급으로_응답을_요구한다() {
        service.handle(message("PROLONGED_INACTIVITY", null));

        assertThat(jdbc.queryForObject("select risk_level from analysis_events", String.class))
                .isEqualTo("WARNING");
        assertThat(jdbc.queryForObject("select count(*) from notifications", Integer.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject("select response_status from notifications", String.class))
                .isEqualTo("PENDING");
    }

    @Test
    void 외출_중에는_부재_기반_이벤트만_억제한다() {
        var now = OffsetDateTime.now(ZoneOffset.UTC);
        jdbc.update("update subjects set monitoring_enabled=false, away_started_at=?, away_until=?",
                now.minusHours(1), now.plusHours(1));

        service.handle(message("PROLONGED_INACTIVITY", null));
        assertThat(jdbc.queryForObject("select count(*) from analysis_events", Integer.class))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from notifications", Integer.class))
                .isZero();

        // 장시간 사용은 집을 비운 상태일수록 위험하므로 외출 중에도 알린다.
        service.handle(message("PROLONGED_APPLIANCE_USE", "IRON"));
        assertThat(jdbc.queryForObject("select count(*) from notifications", Integer.class))
                .isEqualTo(1);
    }

    @Test
    void 대상자_미등록_가구의_이벤트는_저장하지_않고_컨슈머도_멈추지_않는다() {
        // 시뮬레이터 테스트 가구처럼 대상자가 없는 가구의 이벤트도 토픽에 흘러온다.
        // 예외로 컨테이너를 멈추면 다른 가구의 알림까지 끊긴다.
        jdbc.update("delete from subjects");
        assertThatCode(() -> service.handle(message("PROLONGED_APPLIANCE_USE", "KETTLE")))
                .doesNotThrowAnyException();
        assertThat(jdbc.queryForObject("select count(*) from analysis_events", Integer.class))
                .isZero();
        assertThat(jdbc.queryForObject("select count(*) from notifications", Integer.class))
                .isZero();
    }
}

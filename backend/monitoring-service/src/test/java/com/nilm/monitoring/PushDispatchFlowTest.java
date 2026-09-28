package com.nilm.monitoring;

import com.nilm.monitoring.dto.kafka.AnalysisEventMessage;
import com.nilm.monitoring.service.AnalysisEventService;
import com.nilm.monitoring.service.WebPushSender;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;
import nl.martijndwars.webpush.PushService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(properties = {"app.push.enabled=true", "spring.profiles.active=local"})
@AutoConfigureMockMvc
class PushDispatchFlowTest {
    @MockitoBean PushService pushService;
    @MockitoBean WebPushSender sender;
    @Autowired AnalysisEventService service;
    @Autowired JdbcTemplate jdbc;
    @Autowired DataSource dataSource;
    @Autowired MockMvc mvc;

    @BeforeEach
    void setup() {
        jdbc.update("delete from notifications");
        jdbc.update("delete from risk_assessments");
        jdbc.update("delete from analysis_events");
        jdbc.update("delete from subjects");
        jdbc.update("delete from push_subscriptions");
        jdbc.update("""
                insert into subjects(household_id, auth_sub, birth_date, name, phone, address)
                values ('house-push', 'test-subject-3', DATE '1950-01-01', 'test', '010', 'test')
                """);
        jdbc.update("""
                insert into push_subscriptions(auth_sub, endpoint, p256dh, auth)
                values ('test-subject-3', 'https://example.com/push/test', 'test', 'test')
                """);
    }

    /** 즉시 알림 경로를 타는 유형. 계약대로 score는 싣지 않는다. */
    AnalysisEventMessage message() {
        return new AnalysisEventMessage(UUID.randomUUID(), "house-push", null,
                OffsetDateTime.now(), Map.of("allowed_duration_minutes", 60),
                "PROLONGED_APPLIANCE_USE", "KETTLE");
    }

    @Test
    void sendsOnlyAfterCommitAndPersistsSendResult() throws Exception {
        when(sender.send(any(), anyMap(), anyInt())).thenAnswer(invocation -> {
            // 원래 처리 트랜잭션과 다른 DB 연결에서도 저장된 행이 보여야 한다.
            try (var connection = dataSource.getConnection();
                 var statement = connection.createStatement();
                 var result = statement.executeQuery("select count(*) from notifications")) {
                result.next();
                assertThat(result.getInt(1)).isEqualTo(1);
            }
            return 201;
        });
        var event = message();
        service.handle(event);
        service.handle(event);
        verify(sender, times(1)).send(any(), anyMap(), anyInt());
        assertThat(jdbc.queryForObject("select send_status from notifications", String.class)).isEqualTo("SENT");
        // 생성·갱신 시각이 DB 열로 남아야 발송 지연과 마지막 상태 변경 시각을 행에서 바로 읽을 수 있다.
        var createdAt = jdbc.queryForObject("select created_at from notifications", OffsetDateTime.class);
        var updatedAt = jdbc.queryForObject("select updated_at from notifications", OffsetDateTime.class);
        assertThat(createdAt).isNotNull();
        assertThat(updatedAt).isAfterOrEqualTo(createdAt);
    }

    @Test
    void failedPushDoesNotRollBackStoredEvent() throws Exception {
        when(sender.send(any(), anyMap(), anyInt())).thenReturn(503);
        service.handle(message());
        assertThat(jdbc.queryForObject("select count(*) from analysis_events", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select send_status from notifications", String.class)).isEqualTo("FAILED");
    }

    @Test
    void localTestApiActuallyDispatches() throws Exception {
        when(sender.send(any(), anyMap(), anyInt())).thenReturn(201);
        mvc.perform(post("/api/monitoring/push-test"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.subscriptionCount").value(1))
                .andExpect(jsonPath("$.acceptedCount").value(1));
        verify(sender).send(any(), anyMap(), anyInt());
    }
}


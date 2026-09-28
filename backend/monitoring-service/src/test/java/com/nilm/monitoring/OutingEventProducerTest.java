package com.nilm.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nilm.monitoring.service.AwayModeService;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

/**
 * 외출 설정 API와 예약 발효가 {@code monitoring.household-presence.v1} 계약대로
 * 이벤트를 내보내는지 확인한다.
 */
@SpringBootTest
@AutoConfigureMockMvc
class OutingEventProducerTest {

    private static final String ENDPOINT = "/api/monitoring/my-dashboard/away-mode";
    private static final String TOPIC = "monitoring.household-presence.v1";

    @Autowired MockMvc mockMvc;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper objectMapper;
    @Autowired AwayModeService awayModeService;

    @MockitoBean KafkaTemplate<String, String> kafkaTemplate;

    @BeforeEach
    void prepare() {
        CompletableFuture<SendResult<String, String>> sent =
                CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(any(), any(), any())).thenReturn(sent);

        jdbc.update("delete from notifications");
        jdbc.update("delete from analysis_events");
        jdbc.update("delete from subjects");
        jdbc.update("""
                insert into subjects(
                    household_id, auth_sub, birth_date, name, phone, address, monitoring_enabled
                ) values ('H001', 'subject-a', cast('1948-01-01' as date), '김철수',
                    '010-9999-9999', '서울특별시', true)
                """);
    }

    private OffsetDateTime now() {
        return OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS);
    }

    private ResultActions call(String body) throws Exception {
        return mockMvc.perform(put(ENDPOINT)
                .with(jwt().jwt(token -> token.subject("subject-a")))
                .contentType(MediaType.APPLICATION_JSON)
                .content(body));
    }

    /** 마지막으로 발행된 메시지를 계약 필드 그대로 읽는다. */
    private Map<String, Object> lastPayload(int expectedSends) throws Exception {
        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, org.mockito.Mockito.times(expectedSends))
                .send(eq(TOPIC), eq("H001"), payload.capture());

        Map<String, Object> message = objectMapper.readValue(
                payload.getValue(),
                new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});

        // 계약에 없는 필드가 섞이면 AI가 DLQ로 보낸다.
        assertThat(message).containsOnlyKeys(
                "event_id", "household_id", "event_type", "occurred_at");
        assertThat(UUID.fromString((String) message.get("event_id"))).isNotNull();
        assertThat(message.get("household_id")).isEqualTo("H001");
        return message;
    }

    private OffsetDateTime occurredAt(Map<String, Object> message) {
        return OffsetDateTime.parse((String) message.get("occurred_at"));
    }

    @Test
    void immediateAwayPublishesOutingStarted() throws Exception {
        OffsetDateTime before = now();

        call("""
                {"enabled": true}
                """).andExpect(status().isNoContent());

        Map<String, Object> message = lastPayload(1);
        assertThat(message.get("event_type")).isEqualTo("OUTING_STARTED");
        assertThat(occurredAt(message).toInstant()).isAfterOrEqualTo(before.toInstant());
    }

    @Test
    void cancellingWhileAwayPublishesOutingEnded() throws Exception {
        call("""
                {"enabled": true}
                """).andExpect(status().isNoContent());

        call("""
                {"enabled": false}
                """).andExpect(status().isNoContent());

        Map<String, Object> message = lastPayload(2);
        assertThat(message.get("event_type")).isEqualTo("OUTING_ENDED");
    }

    @Test
    void reservationAndItsCancellationPublishNothing() throws Exception {
        call("""
                {"enabled": true, "startsAt": "%s"}
                """.formatted(now().plusHours(3))).andExpect(status().isNoContent());

        call("""
                {"enabled": false}
                """).andExpect(status().isNoContent());

        // 아직 외출이 시작되지 않았으므로 알릴 상태 변화가 없다.
        verify(kafkaTemplate, never()).send(any(), any(), any());
    }

    @Test
    void reschedulingWhileAwayDoesNotRepeatTheSameEvent() throws Exception {
        call("""
                {"enabled": true}
                """).andExpect(status().isNoContent());

        // 외출 중에 종료 시각만 새로 잡는다. 상태는 계속 외출이다.
        call("""
                {"enabled": true, "endsAt": "%s"}
                """.formatted(now().plusHours(2))).andExpect(status().isNoContent());

        Map<String, Object> message = lastPayload(1);
        assertThat(message.get("event_type")).isEqualTo("OUTING_STARTED");
    }

    @Test
    void schedulerPublishesBoundaryTimeNotPublishTime() throws Exception {
        OffsetDateTime startedAt = now().minusHours(1);
        OffsetDateTime endedAt = now().minusMinutes(10);

        // 시작 시각이 지난 예약을 발효시킨다.
        jdbc.update("""
                update subjects set monitoring_enabled = true,
                    away_started_at = ?, away_until = ?
                """, startedAt, now().plusHours(3));
        assertThat(awayModeService.applyScheduledTransitions()).isEqualTo(1);

        Map<String, Object> started = lastPayload(1);
        assertThat(started.get("event_type")).isEqualTo("OUTING_STARTED");
        assertThat(occurredAt(started).toInstant()).isEqualTo(startedAt.toInstant());

        // 종료 시각이 지난 외출을 발효시킨다.
        jdbc.update("""
                update subjects set monitoring_enabled = false,
                    away_started_at = ?, away_until = ?
                """, startedAt, endedAt);
        assertThat(awayModeService.applyScheduledTransitions()).isEqualTo(1);

        Map<String, Object> ended = lastPayload(2);
        assertThat(ended.get("event_type")).isEqualTo("OUTING_ENDED");
        assertThat(occurredAt(ended).toInstant()).isEqualTo(endedAt.toInstant());
    }
}

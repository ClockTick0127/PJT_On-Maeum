package com.nilm.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.nilm.monitoring.service.AwayModeService;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

@SpringBootTest
@AutoConfigureMockMvc
class AwayModeApiTest {

    private static final String ENDPOINT = "/api/monitoring/my-dashboard/away-mode";

    @Autowired MockMvc mockMvc;
    @Autowired JdbcTemplate jdbc;
    @Autowired AwayModeService awayModeService;

    @BeforeEach
    void cleanDatabase() {
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

    private boolean monitoringEnabled() {
        return jdbc.queryForObject(
                "select monitoring_enabled from subjects where auth_sub = 'subject-a'",
                Boolean.class);
    }

    private OffsetDateTime awayStartedAt() {
        return jdbc.queryForObject(
                "select away_started_at from subjects where auth_sub = 'subject-a'",
                OffsetDateTime.class);
    }

    private OffsetDateTime awayUntil() {
        return jdbc.queryForObject(
                "select away_until from subjects where auth_sub = 'subject-a'",
                OffsetDateTime.class);
    }

    private ResultActions call(String body) throws Exception {
        return mockMvc.perform(put(ENDPOINT)
                .with(jwt().jwt(token -> token.subject("subject-a")))
                .contentType(MediaType.APPLICATION_JSON)
                .content(body));
    }

    @Test
    void startsImmediatelyWhenStartTimeIsOmitted() throws Exception {
        OffsetDateTime endsAt = now().plusHours(4);

        call("""
                {"enabled": true, "endsAt": "%s"}
                """.formatted(endsAt))
                .andExpect(status().isNoContent())
                .andExpect(content().string(""));

        assertThat(monitoringEnabled()).isFalse();
        assertThat(awayStartedAt()).isNotNull();
        assertThat(awayUntil()).isEqualTo(endsAt);
    }

    @Test
    void runsUntilCancelledWhenEndTimeIsOmitted() throws Exception {
        call("""
                {"enabled": true}
                """)
                .andExpect(status().isNoContent())
                .andExpect(content().string(""));

        assertThat(monitoringEnabled()).isFalse();
        assertThat(awayStartedAt()).isNotNull();
        assertThat(awayUntil()).isNull();
    }

    @Test
    void futureStartIsScheduledAndKeepsMonitoringOnUntilItBegins() throws Exception {
        OffsetDateTime startsAt = now().plusHours(3);
        OffsetDateTime endsAt = startsAt.plusHours(4);

        call("""
                {"enabled": true, "startsAt": "%s", "endsAt": "%s"}
                """.formatted(startsAt, endsAt))
                .andExpect(status().isNoContent())
                .andExpect(content().string(""));

        // 예약을 걸었을 뿐이므로 지금은 계속 감시해야 한다.
        assertThat(monitoringEnabled()).isTrue();
        assertThat(awayStartedAt()).isEqualTo(startsAt);
        assertThat(awayUntil()).isEqualTo(endsAt);
    }

    @Test
    void cancellingWhileAwayResumesMonitoringAndKeepsTheWindow() throws Exception {
        call("""
                {"enabled": true}
                """).andExpect(status().isNoContent());

        call("""
                {"enabled": false}
                """)
                .andExpect(status().isNoContent())
                .andExpect(content().string(""));

        assertThat(monitoringEnabled()).isTrue();
        // 늦게 도착한 이벤트를 판정할 수 있도록 지나간 구간은 남긴다.
        assertThat(awayStartedAt()).isNotNull();
    }

    @Test
    void cancellingAPendingReservationRemovesIt() throws Exception {
        call("""
                {"enabled": true, "startsAt": "%s"}
                """.formatted(now().plusHours(3))).andExpect(status().isNoContent());

        call("""
                {"enabled": false}
                """)
                .andExpect(status().isNoContent())
                .andExpect(content().string(""));

        assertThat(awayStartedAt()).isNull();
    }

    @Test
    void schedulerAppliesDueStartAndDueEnd() {
        OffsetDateTime past = now().minusHours(1);

        // 시작 시각이 지난 예약 -> 감시를 꺼야 한다.
        jdbc.update("""
                update subjects set monitoring_enabled = true,
                    away_started_at = ?, away_until = ?
                """, past, past.plusHours(3));
        assertThat(awayModeService.applyScheduledTransitions()).isEqualTo(1);
        assertThat(monitoringEnabled()).isFalse();

        // 종료 시각이 지난 외출 -> 감시를 되살려야 한다.
        jdbc.update("""
                update subjects set monitoring_enabled = false,
                    away_started_at = ?, away_until = ?
                """, past.minusHours(3), past);
        assertThat(awayModeService.applyScheduledTransitions()).isEqualTo(1);
        assertThat(monitoringEnabled()).isTrue();

        // 이미 맞아 있으면 아무것도 바꾸지 않는다.
        assertThat(awayModeService.applyScheduledTransitions()).isZero();
    }

    @Test
    void pastStartTimeIsRejected() throws Exception {
        call("""
                {"enabled": true, "startsAt": "%s"}
                """.formatted(now().minusHours(2)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("외출 시작 시간은 현재 시각 이후여야 합니다."));

        assertThat(monitoringEnabled()).isTrue();
    }

    @Test
    void endTimeBeforeStartTimeIsRejected() throws Exception {
        OffsetDateTime startsAt = now().plusHours(3);

        call("""
                {"enabled": true, "startsAt": "%s", "endsAt": "%s"}
                """.formatted(startsAt, startsAt.minusMinutes(30)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("외출 종료 시간은 시작 시간 이후여야 합니다."));
    }

    @Test
    void cancellingWithTimesIsRejected() throws Exception {
        call("""
                {"enabled": false, "endsAt": "%s"}
                """.formatted(now().plusHours(2)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message")
                        .value("외출 모드를 해제할 때는 시간을 지정할 수 없습니다."));
    }

    @Test
    void missingEnabledFlagIsRejected() throws Exception {
        call("""
                {"endsAt": "%s"}
                """.formatted(now().plusHours(2)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"));
    }

    @Test
    void unauthenticatedAndUnknownSubjectRequestsAreRejected() throws Exception {
        mockMvc.perform(put(ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"enabled\": true}"))
                .andExpect(status().isUnauthorized());

        mockMvc.perform(put(ENDPOINT)
                        .with(jwt().jwt(token -> token.subject("unknown-subject")))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"enabled\": true}"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.message").value("대상자 정보를 찾을 수 없습니다."));
    }
}

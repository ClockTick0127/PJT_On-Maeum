package com.nilm.monitoring;

import com.nilm.monitoring.dto.NotificationResponseRequest;
import com.nilm.monitoring.service.NotificationService;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
class SubjectMonitoringApiTest {

    private static final String ENDPOINT = "/api/monitoring/subjects/search";
    private static final String DASHBOARD_ENDPOINT = "/api/monitoring/dashboard";
    private static final ZoneId SEOUL = ZoneId.of("Asia/Seoul");

    @Autowired MockMvc mockMvc;
    @Autowired JdbcTemplate jdbc;
    @Autowired NotificationService notificationService;

    @BeforeEach
    void cleanDatabase() {
        jdbc.update("delete from notifications");
        jdbc.update("delete from analysis_events");
        jdbc.update("delete from subjects");
        jdbc.update("delete from managers");
    }

    @Test
    void returnsOnlyAssignedSubjectsWithMonitoringSummary() throws Exception {
        long managerId = insertManager("manager-a");
        long otherManagerId = insertManager("manager-b");
        LocalDate today = LocalDate.now(SEOUL);
        OffsetDateTime lastActivity = today.atTime(8, 30).atZone(SEOUL).toOffsetDateTime();
        OffsetDateTime updatedAt = today.atTime(9, 0, 26).atZone(SEOUL).toOffsetDateTime();
        long subjectId = insertSubject(
                managerId, "H001", "김철수", "1948-09-18",
                "서울특별시 중구", "101호", "01012345678",
                42, "DANGER", 92, lastActivity, "KETTLE", updatedAt);
        insertSubject(
                otherManagerId, "H002", "다른 대상자", "1950-01-01",
                "부산광역시", null, "01099999999",
                1, "NORMAL", 0, null, null, updatedAt);

        UUID oldEventId = insertEvent(subjectId, "H001", 45, "WARNING",
                today.minusDays(5).atTime(10, 0).atZone(SEOUL).toOffsetDateTime());
        insertEvent(subjectId, "H001", 30, "NORMAL",
                today.minusDays(5).atTime(8, 0).atZone(SEOUL).toOffsetDateTime());
        UUID latestEventId = insertEvent(subjectId, "H001", 92, "DANGER",
                today.atTime(9, 0).atZone(SEOUL).toOffsetDateTime());

        jdbc.update("""
                insert into notifications(
                    event_id, subject_id, auth_sub, response_deadline, user_response,
                    send_status, response_status, responded_at
                ) values (?, ?, 'subject-a', ?, true, 'SENT', 'ANSWERED', ?)
                """, oldEventId, subjectId, updatedAt.plusSeconds(30), updatedAt);
        jdbc.update("""
                insert into notifications(
                    event_id, subject_id, auth_sub, response_deadline, user_response,
                    send_status, response_status, responded_at
                ) values (?, ?, 'subject-a', ?, true, 'SENT', 'ANSWERED', ?)
                """, latestEventId, subjectId, updatedAt.plusSeconds(30), updatedAt);
        long alertId = jdbc.queryForObject(
                "select max(id) from notifications", Long.class);

        mockMvc.perform(get(ENDPOINT)
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.subjects.length()").value(1))
                .andExpect(jsonPath("$.subjects[0].subjectId")
                        .value(Long.toString(subjectId)))
                .andExpect(jsonPath("$.subjects[0].name").value("김철수"))
                .andExpect(jsonPath("$.subjects[0].age").value(
                        Period.between(LocalDate.of(1948, 9, 18), today).getYears()))
                .andExpect(jsonPath("$.subjects[0].address")
                        .value("서울특별시 중구 101호"))
                .andExpect(jsonPath("$.subjects[0].phone").value("01012345678"))
                .andExpect(jsonPath("$.subjects[0].version").value(42))
                .andExpect(jsonPath("$.subjects[0].riskLevel").value("DANGER"))
                .andExpect(jsonPath("$.subjects[0].riskScore").value(92))
                .andExpect(jsonPath("$.subjects[0].lastActivity.applianceType")
                        .value("KETTLE"))
                .andExpect(jsonPath("$.subjects[0].latestAlert.alertId")
                        .value(Long.toString(alertId)))
                .andExpect(jsonPath("$.subjects[0].latestAlert.eventId")
                        .value(latestEventId.toString()))
                .andExpect(jsonPath("$.subjects[0].latestAlert.subjectResponse.status")
                        .value("ANSWERED"))
                .andExpect(jsonPath("$.subjects[0].latestAlert.subjectResponse.answer")
                        .value("YES"))
                .andExpect(jsonPath("$.subjects[0].riskTrend.timezone")
                        .value("Asia/Seoul"))
                .andExpect(jsonPath("$.subjects[0].riskTrend.dailyScores.length()")
                        .value(7))
                .andExpect(jsonPath("$.subjects[0].riskTrend.dailyScores[1].date")
                        .value(today.minusDays(5).toString()))
                .andExpect(jsonPath("$.subjects[0].riskTrend.dailyScores[1].score")
                        .value(45))
                .andExpect(jsonPath("$.subjects[0].riskTrend.dailyScores[6].date")
                        .value(today.toString()))
                .andExpect(jsonPath("$.subjects[0].riskTrend.dailyScores[6].score")
                        .value(92));
    }

    @Test
    void subjectWithoutActivityOrAlertReturnsNullsAndSevenZeroScores() throws Exception {
        long managerId = insertManager("manager-a");
        insertSubject(
                managerId, "H001", "김철수", "1950-01-01",
                "서울특별시", null, "01012345678",
                1, "NORMAL", 0, null, null, OffsetDateTime.now());

        mockMvc.perform(get(ENDPOINT)
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.subjects[0].lastActivity").doesNotExist())
                .andExpect(jsonPath("$.subjects[0].latestAlert").doesNotExist())
                .andExpect(jsonPath("$.subjects[0].riskTrend.dailyScores.length()")
                        .value(7))
                .andExpect(jsonPath("$.subjects[0].riskTrend.dailyScores[*].score")
                        .value(org.hamcrest.Matchers.everyItem(org.hamcrest.Matchers.is(0))));
    }

    @Test
    void unauthenticatedAndUnknownManagerRequestsAreRejected() throws Exception {
        mockMvc.perform(get(ENDPOINT))
                .andExpect(status().isUnauthorized());

        mockMvc.perform(get(ENDPOINT)
                        .with(jwt().jwt(token -> token.subject("unknown-manager"))))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.message")
                        .value("등록된 담당자만 대상자 목록을 조회할 수 있습니다."));
    }

    @Test
    void registeredManagerWithoutSubjectsReceivesEmptyList() throws Exception {
        insertManager("manager-a");

        mockMvc.perform(get(ENDPOINT)
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.subjects.length()").value(0));
    }

    @Test
    void dashboardEndpointReusesTheManagerSummaryAndReturnsCreated() throws Exception {
        insertManager("manager-a");

        mockMvc.perform(get(DASHBOARD_ENDPOINT)
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.subjects.length()").value(0));
    }

    @Test
    void firstNotificationAnswerStoresServerTimeAndAdvancesVersionOnlyOnce() {
        long managerId = insertManager("manager-a");
        long subjectId = insertSubject(
                managerId, "H001", "김철수", "1950-01-01",
                "서울특별시", null, "01012345678",
                41, "WARNING", 80, null, null, OffsetDateTime.now());
        UUID eventId = insertEvent(
                subjectId, "H001", 80, "WARNING", OffsetDateTime.now());
        jdbc.update("""
                insert into notifications(
                    event_id, auth_sub, response_deadline, send_status, response_status
                ) values (?, 'subject-H001', ?, 'SENT', 'PENDING')
                """, eventId, OffsetDateTime.now().plusMinutes(1));
        long notificationId = jdbc.queryForObject(
                "select id from notifications", Long.class);
        var request = new NotificationResponseRequest(
                "yes", "user", OffsetDateTime.now());

        notificationService.respond(notificationId, "subject-H001", request);
        notificationService.respond(notificationId, "subject-H001", request);

        assertThat(jdbc.queryForObject(
                "select state_version from subjects where id = ?",
                Long.class,
                subjectId
        )).isEqualTo(42L);
        assertThat(jdbc.queryForObject(
                "select responded_at from notifications where id = ?",
                OffsetDateTime.class,
                notificationId
        )).isNotNull();
    }

    @Test
    void notificationExpirationAdvancesVersionOnlyOnce() {
        long managerId = insertManager("manager-a");
        long subjectId = insertSubject(
                managerId, "H001", "김철수", "1950-01-01",
                "서울특별시", null, "01012345678",
                41, "WARNING", 80, null, null, OffsetDateTime.now());
        UUID eventId = insertEvent(
                subjectId, "H001", 80, "WARNING", OffsetDateTime.now().minusMinutes(1));
        jdbc.update("""
                insert into notifications(
                    event_id, auth_sub, response_deadline, send_status, response_status
                ) values (?, 'subject-H001', ?, 'SENT', 'PENDING')
                """, eventId, OffsetDateTime.now().minusSeconds(1));

        notificationService.expireOverdue();
        notificationService.expireOverdue();

        assertThat(jdbc.queryForObject(
                "select response_status from notifications", String.class
        )).isEqualTo("EXPIRED");
        assertThat(jdbc.queryForObject(
                "select state_version from subjects where id = ?", Long.class, subjectId
        )).isEqualTo(42L);
    }

    private long insertManager(String authSub) {
        jdbc.update("""
                insert into managers(auth_sub, name, organization)
                values (?, '김담당', '중구 복지관')
                """, authSub);
        return jdbc.queryForObject(
                "select id from managers where auth_sub = ?", Long.class, authSub);
    }

    private long insertSubject(
            long managerId,
            String householdId,
            String name,
            String birthDate,
            String address,
            String addressDetail,
            String phone,
            long version,
            String riskLevel,
            int riskScore,
            OffsetDateTime lastActivityAt,
            String lastActivityAppliance,
            OffsetDateTime updatedAt
    ) {
        jdbc.update("""
                insert into subjects(
                    household_id, auth_sub, birth_date, name, phone, address,
                    address_detail, manager_id, state_version, current_risk_level,
                    current_risk_score, last_activity_at, last_activity_appliance, updated_at
                ) values (?, ?, cast(? as date), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                householdId, "subject-" + householdId, birthDate, name, phone, address,
                addressDetail, managerId, version, riskLevel, riskScore,
                lastActivityAt, lastActivityAppliance, updatedAt);
        return jdbc.queryForObject(
                "select id from subjects where household_id = ?", Long.class, householdId);
    }

    private UUID insertEvent(
            long subjectId,
            String householdId,
            int score,
            String riskLevel,
            OffsetDateTime occurredAt
    ) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
                insert into analysis_events(
                    id, subject_id, household_id, event_type, appliance_type,
                    risk_score, risk_level, occurred_at, reason
                ) values (?, ?, ?, 'ROUTINE_MISSED', 'KETTLE', ?, ?, ?, '{}')
                """, id, subjectId, householdId, score, riskLevel, occurredAt);
        return id;
    }
}

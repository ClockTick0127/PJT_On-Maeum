package com.nilm.monitoring;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDate;
import java.time.OffsetDateTime;
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
class SubjectEventsApiTest {

    private static final ZoneId SEOUL = ZoneId.of("Asia/Seoul");
    private static final String ENDPOINT = "/api/monitoring/subjects/{id}/events";

    @Autowired MockMvc mockMvc;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper objectMapper;

    @BeforeEach
    void cleanDatabase() {
        jdbc.update("delete from notifications");
        jdbc.update("delete from analysis_events");
        jdbc.update("delete from subjects");
        jdbc.update("delete from managers");
    }

    @Test
    void returnsEventsNewestFirstWithTheLinkedAlert() throws Exception {
        long managerId = insertManager("manager-a");
        long subjectId = insertSubject(managerId, "H001", "subject-H001");
        LocalDate today = LocalDate.now(SEOUL);
        OffsetDateTime occurredAt = today.atTime(9, 0).atZone(SEOUL).toOffsetDateTime();

        insertEvent(subjectId, "H001", "ROUTINE_MISSED", "KETTLE", 45, "WARNING",
                today.minusDays(2).atTime(8, 0).atZone(SEOUL).toOffsetDateTime(), "{}");
        UUID latest = insertEvent(subjectId, "H001", "ROUTINE_MISSED", "KETTLE",
                92, "DANGER", occurredAt,
                "{\"expected_until\":\"2026-09-16T23:10:00Z\"}");
        long alertId = insertNotification(
                latest, "subject-H001", "ANSWERED", true,
                occurredAt.plusSeconds(25), "ACKNOWLEDGED", occurredAt.plusMinutes(5));

        mockMvc.perform(get(ENDPOINT, subjectId)
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.subjectId").value(Long.toString(subjectId)))
                .andExpect(jsonPath("$.from").value(today.minusDays(6).toString()))
                .andExpect(jsonPath("$.to").value(today.toString()))
                .andExpect(jsonPath("$.timezone").value("Asia/Seoul"))
                .andExpect(jsonPath("$.events.length()").value(2))
                .andExpect(jsonPath("$.events[0].eventId").value(latest.toString()))
                .andExpect(jsonPath("$.events[0].eventType").value("ROUTINE_MISSED"))
                .andExpect(jsonPath("$.events[0].applianceType").value("KETTLE"))
                .andExpect(jsonPath("$.events[0].description").value("전기포트 미작동 감지"))
                .andExpect(jsonPath("$.events[0].riskLevel").value("DANGER"))
                .andExpect(jsonPath("$.events[0].riskScore").value(92))
                .andExpect(jsonPath("$.events[0].reason.expected_until")
                        .value("2026-09-16T23:10:00Z"))
                .andExpect(jsonPath("$.events[0].alert.alertId")
                        .value(Long.toString(alertId)))
                .andExpect(jsonPath("$.events[0].alert.managerStatus")
                        .value("ACKNOWLEDGED"))
                .andExpect(jsonPath("$.events[0].alert.managerStatusUpdatedAt").exists())
                .andExpect(jsonPath("$.events[0].alert.subjectResponse.status")
                        .value("ANSWERED"))
                .andExpect(jsonPath("$.events[0].alert.subjectResponse.answer")
                        .value("YES"))
                .andExpect(jsonPath("$.events[0].alert.subjectResponse.respondedAt")
                        .exists())
                .andExpect(jsonPath("$.events[1].alert").doesNotExist())
                .andExpect(jsonPath("$.pagination.size").value(20))
                .andExpect(jsonPath("$.pagination.hasNext").value(false))
                .andExpect(jsonPath("$.pagination.nextCursor").doesNotExist());
    }

    @Test
    void walksEveryEventExactlyOnceThroughTheCursor() throws Exception {
        long managerId = insertManager("manager-a");
        long subjectId = insertSubject(managerId, "H001", "subject-H001");
        LocalDate today = LocalDate.now(SEOUL);
        // 같은 시각 이벤트를 섞어 커서가 id로도 순서를 고정하는지 확인한다.
        OffsetDateTime shared = today.atTime(7, 0).atZone(SEOUL).toOffsetDateTime();
        insertEvent(subjectId, "H001", "ROUTINE_MISSED", "KETTLE", 80, "WARNING",
                shared, "{}");
        insertEvent(subjectId, "H001", "ROUTINE_MISSED", "KETTLE", 80, "WARNING",
                shared, "{}");
        insertEvent(subjectId, "H001", "ROUTINE_MISSED", "KETTLE", 80, "WARNING",
                today.atTime(8, 0).atZone(SEOUL).toOffsetDateTime(), "{}");

        var seen = new java.util.ArrayList<String>();
        String cursor = null;
        for (int page = 0; page < 5; page++) {
            var request = get(ENDPOINT, subjectId)
                    .param("size", "2")
                    .with(jwt().jwt(token -> token.subject("manager-a")));
            if (cursor != null) {
                request = request.param("cursor", cursor);
            }
            String body = mockMvc.perform(request)
                    .andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();
            JsonNode json = objectMapper.readTree(body);
            json.get("events").forEach(event -> seen.add(event.get("eventId").asText()));
            if (!json.get("pagination").get("hasNext").asBoolean()) {
                cursor = null;
                break;
            }
            cursor = json.get("pagination").get("nextCursor").asText();
        }

        org.assertj.core.api.Assertions.assertThat(seen).hasSize(3);
        org.assertj.core.api.Assertions.assertThat(seen).doesNotHaveDuplicates();
        org.assertj.core.api.Assertions.assertThat(cursor).isNull();
    }

    @Test
    void filtersByTheRequestedRange() throws Exception {
        long managerId = insertManager("manager-a");
        long subjectId = insertSubject(managerId, "H001", "subject-H001");
        LocalDate today = LocalDate.now(SEOUL);
        insertEvent(subjectId, "H001", "ROUTINE_MISSED", "KETTLE", 80, "WARNING",
                today.minusDays(10).atTime(8, 0).atZone(SEOUL).toOffsetDateTime(), "{}");
        UUID inRange = insertEvent(subjectId, "H001", "ROUTINE_MISSED", "KETTLE",
                80, "WARNING",
                today.minusDays(1).atTime(23, 30).atZone(SEOUL).toOffsetDateTime(), "{}");

        mockMvc.perform(get(ENDPOINT, subjectId)
                        .param("from", today.minusDays(2).toString())
                        .param("to", today.toString())
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.events.length()").value(1))
                .andExpect(jsonPath("$.events[0].eventId").value(inRange.toString()));
    }

    @Test
    void fallsBackToAGenericDescriptionForUnknownEventTypes() throws Exception {
        long managerId = insertManager("manager-a");
        long subjectId = insertSubject(managerId, "H001", "subject-H001");
        LocalDate today = LocalDate.now(SEOUL);
        insertEvent(subjectId, "H001", "SOMETHING_NEW", "MICROWAVE", 80, "WARNING",
                today.atTime(6, 0).atZone(SEOUL).toOffsetDateTime(), "not json");

        mockMvc.perform(get(ENDPOINT, subjectId)
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.events[0].description")
                        .value("전자레인지 사용 이상 감지"))
                .andExpect(jsonPath("$.events[0].reason.raw").value("not json"));
    }

    @Test
    void rejectsOtherManagersAndInvalidParameters() throws Exception {
        long managerId = insertManager("manager-a");
        insertManager("manager-b");
        long subjectId = insertSubject(managerId, "H001", "subject-H001");
        LocalDate today = LocalDate.now(SEOUL);

        mockMvc.perform(get(ENDPOINT, subjectId))
                .andExpect(status().isUnauthorized());

        mockMvc.perform(get(ENDPOINT, subjectId)
                        .with(jwt().jwt(token -> token.subject("manager-b"))))
                .andExpect(status().isForbidden());

        mockMvc.perform(get(ENDPOINT, subjectId)
                        .param("from", today.toString())
                        .param("to", today.minusDays(1).toString())
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isBadRequest());

        mockMvc.perform(get(ENDPOINT, subjectId)
                        .param("size", "0")
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isBadRequest());

        mockMvc.perform(get(ENDPOINT, subjectId)
                        .param("cursor", "not-a-cursor!!")
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isBadRequest());
    }

    private long insertManager(String authSub) {
        jdbc.update("""
                insert into managers(auth_sub, name, organization)
                values (?, '김담당', '중구 복지관')
                """, authSub);
        return jdbc.queryForObject(
                "select id from managers where auth_sub = ?", Long.class, authSub);
    }

    private long insertSubject(long managerId, String householdId, String authSub) {
        jdbc.update("""
                insert into subjects(
                    household_id, auth_sub, birth_date, name, phone, address, manager_id
                ) values (?, ?, cast('1950-01-01' as date), '김철수', '01012345678',
                          '서울특별시 중구', ?)
                """, householdId, authSub, managerId);
        return jdbc.queryForObject(
                "select id from subjects where household_id = ?", Long.class, householdId);
    }

    private UUID insertEvent(
            long subjectId,
            String householdId,
            String eventType,
            String applianceType,
            int score,
            String riskLevel,
            OffsetDateTime occurredAt,
            String reason
    ) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
                insert into analysis_events(
                    id, subject_id, household_id, event_type, appliance_type,
                    risk_score, risk_level, occurred_at, reason
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, id, subjectId, householdId, eventType, applianceType,
                score, riskLevel, occurredAt, reason);
        return id;
    }

    private long insertNotification(
            UUID eventId,
            String authSub,
            String responseStatus,
            Boolean userResponse,
            OffsetDateTime respondedAt,
            String managerStatus,
            OffsetDateTime managerStatusUpdatedAt
    ) {
        jdbc.update("""
                insert into notifications(
                    event_id, auth_sub, response_deadline, user_response,
                    send_status, response_status, responded_at,
                    manager_response_status, manager_status_updated_at
                ) values (?, ?, ?, ?, 'SENT', ?, ?, ?, ?)
                """, eventId, authSub, respondedAt, userResponse,
                responseStatus, respondedAt, managerStatus, managerStatusUpdatedAt);
        return jdbc.queryForObject(
                "select id from notifications where event_id = ?", Long.class, eventId);
    }
}

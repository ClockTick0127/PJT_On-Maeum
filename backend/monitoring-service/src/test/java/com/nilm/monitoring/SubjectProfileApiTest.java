package com.nilm.monitoring;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.nilm.monitoring.dto.kafka.HouseholdProfileMessage;
import com.nilm.monitoring.service.HouseholdProfileService;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
class SubjectProfileApiTest {

    private static final ZoneId SEOUL = ZoneId.of("Asia/Seoul");
    private static final String ENDPOINT = "/api/monitoring/subjects/{id}/profile";

    @Autowired MockMvc mockMvc;
    @Autowired JdbcTemplate jdbc;
    @Autowired HouseholdProfileService service;

    @BeforeEach
    void cleanDatabase() {
        jdbc.update("delete from household_routine_baselines");
        jdbc.update("delete from household_profile_statistics");
        jdbc.update("delete from household_profiles");
        jdbc.update("delete from notifications");
        jdbc.update("delete from analysis_events");
        jdbc.update("delete from subjects");
        jdbc.update("delete from managers");
    }

    @Test
    void showsTheActiveProfileHeaderAndOverallBaselines() throws Exception {
        long managerId = insertManager("manager-a");
        long subjectId = insertSubject(managerId, "H001", "subject-H001");
        LocalDate asOfDate = LocalDate.now(SEOUL).minusDays(1);
        service.receive(profile("H001", "v1", asOfDate));

        mockMvc.perform(get(ENDPOINT, subjectId)
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.subjectId").value(Long.toString(subjectId)))
                .andExpect(jsonPath("$.profileVersion").value("v1"))
                .andExpect(jsonPath("$.asOfDate").value(asOfDate.toString()))
                .andExpect(jsonPath("$.qualityStatus").value("READY"))
                .andExpect(jsonPath("$.ruleVersion").value("gold-profile-v1"))
                // WEEKDAY 기준선은 요약에서 제외한다.
                .andExpect(jsonPath("$.appliances.length()").value(1))
                .andExpect(jsonPath("$.appliances[0].applianceType").value("KETTLE"))
                .andExpect(jsonPath("$.appliances[0].sampleDays").value(26))
                .andExpect(jsonPath("$.appliances[0].expectedUntilSecond").value(33000))
                .andExpect(jsonPath("$.appliances[0].enabled").value(true));
    }

    @Test
    void answersNoContentBeforeAnyProfileArrives() throws Exception {
        long managerId = insertManager("manager-a");
        long subjectId = insertSubject(managerId, "H001", "subject-H001");

        mockMvc.perform(get(ENDPOINT, subjectId)
                        .with(jwt().jwt(token -> token.subject("manager-a"))))
                .andExpect(status().isNoContent());
    }

    @Test
    void rejectsAnonymousAndOtherManagers() throws Exception {
        long managerId = insertManager("manager-a");
        insertManager("manager-b");
        long subjectId = insertSubject(managerId, "H001", "subject-H001");

        mockMvc.perform(get(ENDPOINT, subjectId))
                .andExpect(status().isUnauthorized());

        mockMvc.perform(get(ENDPOINT, subjectId)
                        .with(jwt().jwt(token -> token.subject("manager-b"))))
                .andExpect(status().isForbidden());
    }

    private HouseholdProfileMessage profile(
            String householdId,
            String profileVersion,
            LocalDate asOfDate
    ) {
        return new HouseholdProfileMessage(
                1,
                householdId,
                profileVersion,
                1L,
                "ACTIVE",
                asOfDate,
                asOfDate.minusDays(27),
                asOfDate,
                asOfDate.plusDays(1).atStartOfDay(SEOUL).toOffsetDateTime(),
                asOfDate.atTime(23, 0).atZone(SEOUL).toOffsetDateTime(),
                "snapshot-1",
                "gold-profile-v1",
                "household-statistics-v1-nearest-rank",
                "READY",
                List.of(
                        new HouseholdProfileMessage.RoutineBaseline(
                                "KETTLE", "OVERALL", null, 26, 22,
                                new BigDecimal("0.8462"), new BigDecimal("1.0000"),
                                30600, 33000, 27000, 33000, "READY", true),
                        new HouseholdProfileMessage.RoutineBaseline(
                                "KETTLE", "WEEKDAY", "MON", 4, 4,
                                new BigDecimal("1.0000"), new BigDecimal("0.2857"),
                                30600, 33000, 27000, 33000, "INSUFFICIENT_HISTORY", false)
                ),
                List.of(new HouseholdProfileMessage.Statistic(
                        "CUMULATIVE_ACTIVITY_START_COUNT", null, "ALL", "09:30",
                        20L, 20L, 1.0, 3.0, 0.5, "count", "READY"))
        );
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
}

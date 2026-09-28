package com.nilm.monitoring;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
class MyDashboardApiTest {

    private static final String ENDPOINT = "/api/monitoring/my-dashboard";

    @Autowired MockMvc mockMvc;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void cleanDatabase() {
        jdbc.update("delete from notifications");
        jdbc.update("delete from analysis_events");
        jdbc.update("delete from subjects");
        jdbc.update("delete from managers");
    }

    @Test
    void returnsSubjectAwayModeAndAssignedManager() throws Exception {
        jdbc.update("""
                insert into managers(auth_sub, name, organization, phone)
                values ('manager-a', '이담당', '중구 복지관', '010-1234-5678')
                """);
        long managerId = jdbc.queryForObject(
                "select id from managers where auth_sub = 'manager-a'", Long.class);
        // 고정 시각을 쓰면 실행 시점에 따라 외출이 이미 끝난 것으로 판정된다.
        // 분 단위로 자른다. 초가 0인 시각을 늘 거쳐야 직렬화 형식 차이가 실행 시각에 따라
        // 숨었다 드러나지 않는다(toString()은 0초를 생략하고 Jackson은 붙인다).
        OffsetDateTime startedAt = OffsetDateTime.now(ZoneOffset.UTC)
                .minusHours(1).truncatedTo(ChronoUnit.MINUTES);
        OffsetDateTime until = startedAt.plusHours(2);
        jdbc.update("""
                insert into subjects(
                    household_id, auth_sub, birth_date, name, phone, address,
                    monitoring_enabled, away_started_at, away_until, manager_id
                ) values ('H001', 'subject-a', cast('1948-01-01' as date), '김철수',
                    '010-9999-9999', '서울특별시', false, ?, ?, ?)
                """, startedAt, until, managerId);
        long subjectId = jdbc.queryForObject(
                "select id from subjects where auth_sub = 'subject-a'", Long.class);

        mockMvc.perform(get(ENDPOINT)
                        .with(jwt().jwt(token -> token.subject("subject-a"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.subjectId").value(Long.toString(subjectId)))
                .andExpect(jsonPath("$.name").value("김철수"))
                .andExpect(jsonPath("$.awayMode.enabled").value(true))
                .andExpect(jsonPath("$.awayMode.scheduled").value(false))
                .andExpect(jsonPath("$.awayMode.startedAt").value(iso(startedAt)))
                .andExpect(jsonPath("$.awayMode.until").value(iso(until)))
                .andExpect(jsonPath("$.manager.name").value("이담당"))
                .andExpect(jsonPath("$.manager.phone").value("010-1234-5678"));
    }

    /** Jackson이 OffsetDateTime을 쓰는 형식. 초가 0이어도 초를 적는다. */
    private static String iso(OffsetDateTime at) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(at);
    }

    @Test
    void inactiveAwayModeReturnsDisabledWithNullTimes() throws Exception {
        jdbc.update("""
                insert into subjects(
                    household_id, auth_sub, birth_date, name, phone, address,
                    monitoring_enabled
                ) values ('H001', 'subject-a', cast('1948-01-01' as date), '김철수',
                    '010-9999-9999', '서울특별시', true)
                """);

        mockMvc.perform(get(ENDPOINT)
                        .with(jwt().jwt(token -> token.subject("subject-a"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.awayMode.enabled").value(false))
                .andExpect(jsonPath("$.awayMode.scheduled").value(false))
                .andExpect(jsonPath("$.awayMode.startedAt").doesNotExist())
                .andExpect(jsonPath("$.awayMode.until").doesNotExist())
                .andExpect(jsonPath("$.manager").doesNotExist());
    }

    @Test
    void unauthenticatedAndUnknownSubjectRequestsAreRejected() throws Exception {
        mockMvc.perform(get(ENDPOINT))
                .andExpect(status().isUnauthorized());

        mockMvc.perform(get(ENDPOINT)
                        .with(jwt().jwt(token -> token.subject("unknown-subject"))))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.message").value("대상자 정보를 찾을 수 없습니다."));
    }
}

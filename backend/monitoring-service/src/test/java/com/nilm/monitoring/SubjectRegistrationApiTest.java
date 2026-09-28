package com.nilm.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
class SubjectRegistrationApiTest {

    private static final String ENDPOINT = "/api/monitoring/subjects";

    // 서비스는 Asia/Seoul 기준으로 미래 날짜를 판정하므로 테스트도 같은 기준을 쓴다.
    // 시스템 기본 시간대(UTC 빌드 컨테이너)를 쓰면 한국 00~09시 빌드에서 실패한다.
    private static final ZoneId SEOUL = ZoneId.of("Asia/Seoul");

    @Autowired
    MockMvc mockMvc;

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void cleanDatabase() {
        jdbc.update("delete from notifications");
        jdbc.update("delete from analysis_events");
        jdbc.update("delete from subjects");
        jdbc.update("delete from managers");
    }

    @Test
    void registeredManagerCreatesSubjectAndNormalizedValuesAreStored() throws Exception {
        long managerId = insertManager("manager-sub");

        mockMvc.perform(post(ENDPOINT)
                        .with(jwt().jwt(jwt -> jwt.subject("manager-sub")))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "name": "  홍길동  ",
                                  "birthDate": "1950-01-02",
                                  "phone": "010-1234-5678",
                                  "address": "  서울특별시 중구  ",
                                  "addressDetail": "   ",
                                  "householdId": "  H001  ",
                                  "managerMemo": "  주 1회 방문  "
                                }
                                """))
                .andExpect(status().isCreated())
                .andExpect(content().string(""));

        Map<String, Object> stored = jdbc.queryForMap("select * from subjects");
        assertThat(stored)
                .containsEntry("name", "홍길동")
                .containsEntry("phone", "01012345678")
                .containsEntry("address", "서울특별시 중구")
                .containsEntry("household_id", "H001")
                .containsEntry("manager_memo", "주 1회 방문")
                .containsEntry("manager_id", managerId)
                .containsEntry("monitoring_enabled", true);
        assertThat(stored.get("address_detail")).isNull();
        assertThat(stored.get("auth_sub")).isNull();
        assertThat(stored.get("birth_date").toString()).isEqualTo("1950-01-02");
    }

    @Test
    void unauthenticatedRequestIsRejected() throws Exception {
        mockMvc.perform(post(ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validRequest("H001")))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.code").value("REQUEST_REJECTED"));

        assertSubjectCount(0);
    }

    @Test
    void authenticatedAccountThatIsNotARegisteredManagerIsForbidden() throws Exception {
        mockMvc.perform(post(ENDPOINT)
                        .with(jwt().jwt(jwt -> jwt.subject("unknown-sub")))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validRequest("H001")))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.message")
                        .value("등록된 담당자만 대상자를 등록할 수 있습니다."));

        assertSubjectCount(0);
    }

    @Test
    void invalidFieldsReturnFieldErrorsWithoutSaving() throws Exception {
        insertManager("manager-sub");

        mockMvc.perform(post(ENDPOINT)
                        .with(jwt().jwt(jwt -> jwt.subject("manager-sub")))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "name": "   ",
                                  "birthDate": null,
                                  "phone": "1234",
                                  "address": "",
                                  "householdId": ""
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
                .andExpect(jsonPath("$.errors[?(@.field == 'name')]").exists())
                .andExpect(jsonPath("$.errors[?(@.field == 'birthDate')]").exists())
                .andExpect(jsonPath("$.errors[?(@.field == 'phone')]").exists())
                .andExpect(jsonPath("$.errors[?(@.field == 'address')]").exists())
                .andExpect(jsonPath("$.errors[?(@.field == 'householdId')]").exists());

        assertSubjectCount(0);
    }

    @Test
    void futureBirthDateIsRejectedWithoutSaving() throws Exception {
        insertManager("manager-sub");
        String tomorrow = LocalDate.now(SEOUL).plusDays(1).toString();

        mockMvc.perform(post(ENDPOINT)
                        .with(jwt().jwt(jwt -> jwt.subject("manager-sub")))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validRequest("H001").replace("1950-01-02", tomorrow)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message")
                        .value("생년월일은 오늘 또는 과거 날짜여야 합니다."));

        assertSubjectCount(0);
    }

    @Test
    void malformedBirthDateIsAClientErrorWithoutSaving() throws Exception {
        insertManager("manager-sub");

        mockMvc.perform(post(ENDPOINT)
                        .with(jwt().jwt(jwt -> jwt.subject("manager-sub")))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validRequest("H001")
                                .replace("1950-01-02", "1950/01/02")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("MALFORMED_REQUEST"))
                .andExpect(jsonPath("$.path").value(ENDPOINT));

        assertSubjectCount(0);
    }

    @Test
    void duplicateHouseholdIsRejectedAndOriginalSubjectIsPreserved() throws Exception {
        insertManager("manager-sub");

        mockMvc.perform(post(ENDPOINT)
                        .with(jwt().jwt(jwt -> jwt.subject("manager-sub")))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validRequest("H001")))
                .andExpect(status().isCreated());

        mockMvc.perform(post(ENDPOINT)
                        .with(jwt().jwt(jwt -> jwt.subject("manager-sub")))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validRequest("H001").replace("홍길동", "김영희")))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message")
                        .value("이미 대상자에게 연결된 가구입니다."));

        assertSubjectCount(1);
        assertThat(jdbc.queryForObject("select name from subjects", String.class))
                .isEqualTo("홍길동");
    }

    private long insertManager(String authSub) {
        jdbc.update("""
                insert into managers(auth_sub, name, organization)
                values (?, '김담당', '중구 복지관')
                """, authSub);
        return jdbc.queryForObject(
                "select id from managers where auth_sub = ?",
                Long.class,
                authSub
        );
    }

    private void assertSubjectCount(int expected) {
        assertThat(jdbc.queryForObject(
                "select count(*) from subjects",
                Integer.class
        )).isEqualTo(expected);
    }

    private String validRequest(String householdId) {
        return """
                {
                  "name": "홍길동",
                  "birthDate": "1950-01-02",
                  "phone": "010-1234-5678",
                  "address": "서울특별시 중구",
                  "addressDetail": "101호",
                  "householdId": "%s",
                  "managerMemo": "주 1회 방문"
                }
                """.formatted(householdId);
    }
}

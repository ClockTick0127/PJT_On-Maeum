package com.nilm.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;
import java.util.Map;
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
class ManagerProfileApiTest {

    private static final String ENDPOINT = "/api/monitoring/me";

    @Autowired MockMvc mockMvc;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void cleanDatabase() {
        jdbc.update("delete from notification_settings");
        jdbc.update("delete from notifications");
        jdbc.update("delete from analysis_events");
        jdbc.update("delete from subjects");
        jdbc.update("delete from managers");
        insertManager("manager-a", "old@example.com", "기존 기관");
    }

    @Test
    void updatesProfileAndNotificationPreferences() throws Exception {
        call("""
                {
                  "pushEnabled": true,
                  "dailyReportEnabled": false,
                  "email": "  Manager@Example.com  ",
                  "organization": "  서울시 복지센터  "
                }
                """)
                .andExpect(status().isNoContent())
                .andExpect(content().string(""));

        Map<String, Object> manager = jdbc.queryForMap(
                "select email, organization from managers where auth_sub = 'manager-a'");
        assertThat(manager)
                .containsEntry("email", "manager@example.com")
                .containsEntry("organization", "서울시 복지센터");

        List<Map<String, Object>> settings = jdbc.queryForList("""
                select notification_type, channel, enabled
                from notification_settings
                order by notification_type
                """);
        assertThat(settings).hasSize(3);
        assertSetting(settings, "DANGER", true);
        assertSetting(settings, "WARNING", true);
        assertSetting(settings, "DAILY_SUMMARY", false);
    }

    @Test
    void updatesOnlyProvidedFields() throws Exception {
        call("""
                {"dailyReportEnabled": true}
                """)
                .andExpect(status().isNoContent());

        Map<String, Object> manager = jdbc.queryForMap(
                "select email, organization from managers where auth_sub = 'manager-a'");
        assertThat(manager)
                .containsEntry("email", "old@example.com")
                .containsEntry("organization", "기존 기관");
        assertThat(jdbc.queryForObject(
                "select count(*) from notification_settings", Integer.class)).isEqualTo(1);
    }

    @Test
    void rejectsEmptyAndInvalidRequests() throws Exception {
        call("{}")
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message")
                        .value("수정할 항목을 하나 이상 입력해야 합니다."));

        call("""
                {"email": "not-an-email", "organization": "   "}
                """)
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
                .andExpect(jsonPath("$.errors[?(@.field == 'email')]").exists())
                .andExpect(jsonPath("$.errors[?(@.field == 'organization')]").exists());
    }

    @Test
    void rejectsDuplicateEmailIgnoringCase() throws Exception {
        insertManager("manager-b", "used@example.com", "다른 기관");

        call("""
                {"email": "USED@example.com"}
                """)
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message").value("이미 사용 중인 이메일입니다."));
    }

    @Test
    void rejectsUnauthenticatedAndUnknownManagerRequests() throws Exception {
        mockMvc.perform(patch(ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"pushEnabled\": true}"))
                .andExpect(status().isUnauthorized());

        mockMvc.perform(patch(ENDPOINT)
                        .with(jwt().jwt(token -> token.subject("unknown-manager")))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"pushEnabled\": true}"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.message")
                        .value("등록된 담당자만 계정을 수정할 수 있습니다."));
    }

    private ResultActions call(String body) throws Exception {
        return mockMvc.perform(patch(ENDPOINT)
                .with(jwt().jwt(token -> token.subject("manager-a")))
                .contentType(MediaType.APPLICATION_JSON)
                .content(body));
    }

    private void insertManager(String authSub, String email, String organization) {
        jdbc.update("""
                insert into managers(auth_sub, name, organization, email)
                values (?, '담당자', ?, ?)
                """, authSub, organization, email);
    }

    private void assertSetting(
            List<Map<String, Object>> settings,
            String type,
            boolean enabled
    ) {
        assertThat(settings).anySatisfy(setting -> assertThat(setting)
                .containsEntry("notification_type", type)
                .containsEntry("channel", "PUSH")
                .containsEntry("enabled", enabled));
    }
}

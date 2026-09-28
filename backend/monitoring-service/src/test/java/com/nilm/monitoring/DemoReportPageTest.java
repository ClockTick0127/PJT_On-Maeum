package com.nilm.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(properties = {
        "app.security.enabled=true",
        "app.demo-report.datasource.jdbc-url=jdbc:h2:mem:demo_report;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "app.demo-report.datasource.username=sa",
        "app.demo-report.datasource.password="
})
@AutoConfigureMockMvc
class DemoReportPageTest {
    @Autowired MockMvc mvc;
    @Autowired JdbcTemplate operationalJdbc;
    @Autowired @Qualifier("demoReportJdbcTemplate") JdbcTemplate reports;
    @Autowired DataSource operationalSource;
    @Autowired @Qualifier("demoReportDataSource") DataSource reportSource;
    @MockitoBean JwtDecoder jwtDecoder;

    @BeforeEach
    void prepareViews() {
        reports.execute("DROP SCHEMA IF EXISTS reporting CASCADE");
        reports.execute("CREATE SCHEMA reporting");
        // These tables model the typed columns returned by the PostgreSQL serving views.
        reports.execute("CREATE TABLE reporting.v_report_index (report_id varchar, household_id varchar, period_start date, period_end date, generated_at timestamp, assessment_mode varchar)");
        reports.execute("CREATE TABLE reporting.v_report_summary (report_id varchar, household_id varchar, report_date date, statement varchar)");
        reports.execute("CREATE TABLE reporting.v_appliance_daily (report_id varchar, household_id varchar, usage_date date, appliance_type varchar, usage_minutes double precision, usage_count bigint, usage_status varchar)");
        reports.execute("CREATE TABLE reporting.v_report_evidence (report_id varchar, household_id varchar, evidence_type varchar, appliance_type varchar, statement varchar, quality_status varchar)");
        reports.execute("CREATE TABLE reporting.v_household_daily (report_id varchar, household_id varchar, usage_date date, max_valid_score double precision, max_partial_score double precision)");
        reports.update("INSERT INTO reporting.v_report_index VALUES ('old','H001','2026-08-01','2026-09-22','2026-09-27 10:00:00','EVENT_TIME_REASSESSMENT'), ('latest','H001','2026-08-01','2026-09-23','2026-09-24 10:00:00','EVENT_TIME_REASSESSMENT'), ('earlier-revision','H001','2026-08-01','2026-09-23','2026-09-24 09:00:00','EVENT_TIME_REASSESSMENT'), ('other','H002','2026-08-01','2026-09-26','2026-09-27 11:00:00','EVENT_TIME_REASSESSMENT')");
        reports.update("INSERT INTO reporting.v_report_summary VALUES ('latest','H001','2026-09-23',?), ('old','H001','2026-09-22','OLD_SUMMARY'), ('latest','H002','2026-09-23','OTHER_HOUSEHOLD')", "생활 요약 <script>alert(1)</script>");
        reports.update("INSERT INTO reporting.v_appliance_daily VALUES ('latest','H001','2026-09-23','KETTLE',12.5,3,'COMPLETE')");
        reports.update("INSERT INTO reporting.v_report_evidence VALUES ('latest','H001','BASELINE',null,'기준선 근거','READY')");
        reports.update("INSERT INTO reporting.v_household_daily VALUES ('latest','H001','2026-09-23',42,null), ('latest','H001','2026-09-22',0,12)");
    }

    @Test
    void rendersLatestHouseholdSnapshotAnonymouslyWithEscapedTextAndNullableScores() throws Exception {
        String html = mvc.perform(get("/reports/demo/H001"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(html).contains("생활 요약 &lt;script&gt;", "KETTLE", "12.5", "기준선 근거", "보고서 버전 latest");
        assertThat(html).contains("const dates = [\"2026-09-22\",\"2026-09-23\"]", "const validScores = [0.0,42.0]", "const partialScores = [12.0,null]");
        assertThat(html).doesNotContain("OLD_SUMMARY", "OTHER_HOUSEHOLD", "<script>alert(1)</script>");
    }

    @Test
    void doesNotOpenOtherEndpointsAndReturnsNotFoundForUnknownHouseholds() throws Exception {
        mvc.perform(get("/api/monitoring/subjects/search")).andExpect(status().isUnauthorized());
        mvc.perform(get("/reports/demo/H001/extra")).andExpect(status().isUnauthorized());
        mvc.perform(get("/reports/demo/unknown")).andExpect(status().isNotFound());
    }

    @Test
    void paginatesLargeEvidenceTables() throws Exception {
        for (int i = 0; i < 101; i++) {
            reports.update("INSERT INTO reporting.v_report_evidence VALUES ('latest','H001','DETAIL',null,?,'READY')", "근거-" + String.format("%03d", i));
        }
        String first = mvc.perform(get("/reports/demo/H001"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(first).contains("근거-098", "다음 근거").doesNotContain("근거-099");
        String next = mvc.perform(get("/reports/demo/H001?evidencePage=1"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(next).contains("근거-099", "근거-100", "이전 근거").doesNotContain("다음 근거");
        mvc.perform(get("/reports/demo/H001?evidencePage=-1")).andExpect(status().isBadRequest());
    }

    @Test
    void keepsOperationalDatabaseAndMigrationsSeparate() throws Exception {
        assertThat(operationalSource).isNotSameAs(reportSource);
        assertThat(operationalJdbc.getDataSource()).isSameAs(operationalSource);
        try (var connection = operationalSource.getConnection()) {
            assertThat(connection.getMetaData().getURL()).contains("monitoring_db");
        }
        Integer migrations = reports.queryForObject("SELECT count(*) FROM information_schema.tables WHERE table_name='flyway_schema_history'", Integer.class);
        assertThat(migrations).isZero();
    }
}

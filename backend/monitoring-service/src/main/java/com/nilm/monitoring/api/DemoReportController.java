package com.nilm.monitoring.api;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.server.ResponseStatusException;

@Controller
public class DemoReportController {
    private static final String LATEST_REPORT = """
            WHERE household_id = ? AND report_id = (
                SELECT report_id FROM reporting.v_report_index
                WHERE household_id = ?
                ORDER BY period_end DESC, generated_at DESC, report_id DESC
                LIMIT 1
            )
            """;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate snapshot;

    public DemoReportController(@Qualifier("demoReportJdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
        // A local manager avoids replacing the operational JPA transaction manager.
        this.snapshot = new TransactionTemplate(new DataSourceTransactionManager(
                Objects.requireNonNull(jdbc.getDataSource())));
        this.snapshot.setReadOnly(true);
        this.snapshot.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
        this.snapshot.setTimeout(30);
    }

    @GetMapping("/reports/demo/{householdId}")
    public String report(@PathVariable String householdId,
                         @RequestParam(defaultValue = "0") int evidencePage, Model model) {
        if (evidencePage < 0 || evidencePage > 10000) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "올바른 근거 페이지를 선택해 주세요.");
        }
        snapshot.executeWithoutResult(status -> {
            List<Map<String, Object>> reports = query("""
                    SELECT report_id, household_id, period_start, period_end,
                           generated_at, assessment_mode
                    FROM reporting.v_report_index
                    """, householdId, "LIMIT 1");
            if (reports.isEmpty()) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "가구의 보고서가 없습니다.");
            }
            model.addAttribute("report", reports.getFirst());
            model.addAttribute("summaries", query("""
                    SELECT report_date, statement FROM reporting.v_report_summary
                    """, householdId, "ORDER BY report_date, statement"));
            model.addAttribute("appliances", query("""
                    SELECT usage_date, appliance_type, usage_minutes, usage_count, usage_status
                    FROM reporting.v_appliance_daily
                    """, householdId, "ORDER BY usage_date DESC, appliance_type"));
            List<Map<String, Object>> evidence = query("""
                    SELECT evidence_type, appliance_type, statement, quality_status
                    FROM reporting.v_report_evidence
                    """, householdId, "ORDER BY evidence_type, appliance_type, statement, quality_status LIMIT 101 OFFSET " + evidencePage * 100);
            model.addAttribute("evidence", evidence.subList(0, Math.min(100, evidence.size())));
            model.addAttribute("evidencePage", evidencePage);
            model.addAttribute("hasNextEvidence", evidence.size() > 100);
            List<Map<String, Object>> daily = query("""
                    SELECT usage_date, max_valid_score, max_partial_score
                    FROM reporting.v_household_daily
                    """, householdId, "ORDER BY usage_date");
            model.addAttribute("scoreDates", daily.stream()
                    .map(row -> row.get("usage_date").toString()).toList());
            model.addAttribute("validScores", daily.stream()
                    .map(row -> row.get("max_valid_score")).toList());
            model.addAttribute("partialScores", daily.stream()
                    .map(row -> row.get("max_partial_score")).toList());
        });
        return "reports/demo";
    }

    private List<Map<String, Object>> query(String select, String householdId, String order) {
        return jdbc.queryForList(select + LATEST_REPORT + order, householdId, householdId);
    }
}

package com.nilm.monitoring.dto;

import com.nilm.monitoring.config.enums.RiskLevel;
import com.nilm.monitoring.domain.Notification;
import com.nilm.monitoring.risk.AssessmentStatus;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;

public record SubjectMonitoringResponse(List<SubjectSummary> subjects) {

    /**
     * @param riskLevel 유효 등급. 자체 평가 등급과 이벤트 등급 중 높은 쪽이다
     * @param assessmentStatus 마지막 자체 평가가 성립했는지. 평가한 적이 없으면 null
     * @param confidence 마지막 자체 평가의 신뢰도(0~1). 점수에 곱한 값이 아니다
     * @param riskSource 유효 등급을 만든 쪽(ASSESSMENT/EVENT/NONE)
     */
    public record SubjectSummary(
            String subjectId,
            String name,
            int age,
            String address,
            String phone,
            long version,
            RiskLevel riskLevel,
            int riskScore,
            AssessmentStatus assessmentStatus,
            Double confidence,
            String riskSource,
            LastActivity lastActivity,
            LatestAlert latestAlert,
            RiskTrend riskTrend,
            OffsetDateTime updatedAt
    ) {
    }

    public record LastActivity(
            OffsetDateTime occurredAt,
            String applianceType
    ) {
    }

    public record LatestAlert(
            String alertId,
            String eventId,
            SubjectResponse subjectResponse
    ) {
    }

    public record SubjectResponse(
            Notification.ResponseStatus status,
            String answer,
            OffsetDateTime respondedAt
    ) {
    }

    public record RiskTrend(
            String timezone,
            List<DailyScore> dailyScores
    ) {
    }

    public record DailyScore(LocalDate date, int score) {
    }
}

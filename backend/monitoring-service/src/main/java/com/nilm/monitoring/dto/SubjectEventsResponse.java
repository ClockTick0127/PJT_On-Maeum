package com.nilm.monitoring.dto;

import com.nilm.monitoring.config.enums.RiskLevel;
import com.nilm.monitoring.domain.Notification;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

/**
 * 대상자 상세 화면의 이상 징후 기록 영역 응답.
 * DB의 위험 이벤트(analysis_events)를 발생 시각 최신순으로 내려보낸다.
 */
public record SubjectEventsResponse(
        String subjectId,
        LocalDate from,
        LocalDate to,
        String timezone,
        List<EventItem> events,
        Pagination pagination
) {

    public record EventItem(
            String eventId,
            String eventType,
            String applianceType,
            String description,
            RiskLevel riskLevel,
            int riskScore,
            OffsetDateTime occurredAt,
            Map<String, Object> reason,
            Alert alert
    ) {
    }

    /** 이벤트에 연결된 알림. 알림이 만들어지지 않았으면 null. */
    public record Alert(
            String alertId,
            Notification.ManagerResponseStatus managerStatus,
            OffsetDateTime managerStatusUpdatedAt,
            SubjectResponse subjectResponse
    ) {
    }

    public record SubjectResponse(
            Notification.ResponseStatus status,
            String answer,
            OffsetDateTime respondedAt
    ) {
    }

    public record Pagination(
            int size,
            boolean hasNext,
            String nextCursor
    ) {
    }
}

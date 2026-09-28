package com.nilm.monitoring.service;

import com.nilm.monitoring.domain.AnalysisEvent;
import com.nilm.monitoring.domain.Notification;
import com.nilm.monitoring.domain.RiskAssessmentRecord;
import com.nilm.monitoring.domain.Subject;
import com.nilm.monitoring.dto.SubjectMonitoringResponse;
import com.nilm.monitoring.repository.AnalysisEventRepository;
import com.nilm.monitoring.repository.ManagerRepository;
import com.nilm.monitoring.repository.NotificationRepository;
import com.nilm.monitoring.repository.RiskAssessmentRecordRepository;
import com.nilm.monitoring.repository.SubjectRepository;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
@RequiredArgsConstructor
public class SubjectMonitoringService {

    private static final ZoneId TREND_ZONE = ZoneId.of("Asia/Seoul");
    private static final int TREND_DAYS = 7;

    private final ManagerRepository managers;
    private final SubjectRepository subjects;
    private final AnalysisEventRepository events;
    private final NotificationRepository notifications;
    private final RiskAssessmentRecordRepository assessments;

    @Transactional(readOnly = true)
    public SubjectMonitoringResponse getSubjects(String managerAuthSub) {
        if (managerAuthSub == null || managerAuthSub.isBlank()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "로그인이 필요합니다.");
        }

        var manager = managers.findByAuthSub(managerAuthSub)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.FORBIDDEN,
                        "등록된 담당자만 대상자 목록을 조회할 수 있습니다."
                ));

        LocalDate today = LocalDate.now(TREND_ZONE);
        OffsetDateTime from = today.minusDays(TREND_DAYS - 1L)
                .atStartOfDay(TREND_ZONE)
                .toOffsetDateTime();
        OffsetDateTime until = today.plusDays(1)
                .atStartOfDay(TREND_ZONE)
                .toOffsetDateTime();

        List<SubjectMonitoringResponse.SubjectSummary> result =
                subjects.findAllByManagerIdOrderByIdAsc(manager.getId()).stream()
                        .map(subject -> toSummary(subject, today, from, until))
                        .toList();
        return new SubjectMonitoringResponse(result);
    }

    private SubjectMonitoringResponse.SubjectSummary toSummary(
            Subject subject,
            LocalDate today,
            OffsetDateTime from,
            OffsetDateTime until
    ) {
        List<AnalysisEvent> trendEvents = events
                .findAllBySubjectIdAndOccurredAtGreaterThanEqualAndOccurredAtLessThan(
                        subject.getId(), from, until);
        List<RiskAssessmentRecord> trendAssessments = assessments
                .findAllBySubjectIdAndAssessedAtGreaterThanEqualAndAssessedAtLessThan(
                        subject.getId(), from, until);

        SubjectMonitoringResponse.LastActivity lastActivity =
                subject.getLastActivityAt() == null ? null
                        : new SubjectMonitoringResponse.LastActivity(
                                subject.getLastActivityAt(),
                                subject.getLastActivityAppliance()
                        );

        SubjectMonitoringResponse.LatestAlert latestAlert = notifications
                .findLatestAlertBySubjectId(subject.getId())
                .map(this::toLatestAlert)
                .orElse(null);

        return new SubjectMonitoringResponse.SubjectSummary(
                subject.getId().toString(),
                subject.getName(),
                Period.between(subject.getBirthDate(), today).getYears(),
                joinAddress(subject.getAddress(), subject.getAddressDetail()),
                subject.getPhone(),
                subject.getStateVersion(),
                subject.getCurrentRiskLevel(),
                subject.getCurrentRiskScore(),
                subject.getAssessmentStatus(),
                subject.getAssessmentConfidence(),
                subject.riskSource(),
                lastActivity,
                latestAlert,
                toRiskTrend(today, trendEvents, trendAssessments),
                subject.getUpdatedAt()
        );
    }

    private SubjectMonitoringResponse.LatestAlert toLatestAlert(Notification notification) {
        String answer = notification.getUserResponse() == null ? null
                : notification.getUserResponse() ? "YES" : "NO";
        var response = new SubjectMonitoringResponse.SubjectResponse(
                notification.getResponseStatus(),
                answer,
                notification.getRespondedAt()
        );
        return new SubjectMonitoringResponse.LatestAlert(
                notification.getId().toString(),
                // 자체 평가가 만든 알림은 분석 이벤트에 걸리지 않는다.
                notification.getEventId() == null ? null : notification.getEventId().toString(),
                response
        );
    }

    /**
     * 위험 추이.
     *
     * <p>분석 서비스 이벤트의 점수와 모니터링 자체 평가의 점수를 함께 본다.
     * 자체 평가로만 등급이 올라간 날이 그래프에서 비어 보이면 안 되기 때문이다.
     * 평가하지 못한 날(점수 null)은 0으로 채우지 않고 건너뛴다.
     */
    private SubjectMonitoringResponse.RiskTrend toRiskTrend(
            LocalDate today,
            List<AnalysisEvent> trendEvents,
            List<RiskAssessmentRecord> trendAssessments
    ) {
        Map<LocalDate, Integer> scores = new LinkedHashMap<>();
        for (int offset = TREND_DAYS - 1; offset >= 0; offset--) {
            scores.put(today.minusDays(offset), 0);
        }
        for (AnalysisEvent event : trendEvents) {
            LocalDate eventDate = event.getOccurredAt().atZoneSameInstant(TREND_ZONE).toLocalDate();
            scores.computeIfPresent(eventDate, (date, current) -> Math.max(current, event.getRiskScore()));
        }
        for (RiskAssessmentRecord record : trendAssessments) {
            if (record.getRiskScore() == null) {
                continue;
            }
            LocalDate assessedDate =
                    record.getAssessedAt().atZoneSameInstant(TREND_ZONE).toLocalDate();
            scores.computeIfPresent(
                    assessedDate, (date, current) -> Math.max(current, record.getRiskScore()));
        }
        List<SubjectMonitoringResponse.DailyScore> dailyScores = scores.entrySet().stream()
                .map(entry -> new SubjectMonitoringResponse.DailyScore(
                        entry.getKey(), entry.getValue()))
                .toList();
        return new SubjectMonitoringResponse.RiskTrend(TREND_ZONE.getId(), dailyScores);
    }

    private String joinAddress(String address, String detail) {
        if (detail == null || detail.isBlank()) {
            return address;
        }
        return address + " " + detail;
    }
}

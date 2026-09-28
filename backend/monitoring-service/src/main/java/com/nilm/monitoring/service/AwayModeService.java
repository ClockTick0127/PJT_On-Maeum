package com.nilm.monitoring.service;

import com.nilm.monitoring.config.enums.OutingEventType;
import com.nilm.monitoring.config.enums.StateChangeTrigger;
import com.nilm.monitoring.domain.Subject;
import com.nilm.monitoring.dto.AwayModeRequest;
import com.nilm.monitoring.repository.SubjectRepository;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

/**
 * 대상자 본인이 설정하는 외출 모드.
 *
 * <p>진실의 원천은 {@code away_started_at}~{@code away_until} 구간이고,
 * {@code monitoring_enabled}는 그 구간을 현재 시각으로 접은 캐시다.
 * 예약된 시작·종료는 {@link #applyScheduledTransitions()}가 발효시킨다.
 *
 * <p>AI 분석 서비스로 나가는 외출 이벤트도 이 캐시가 뒤집히는 순간에만 낸다.
 * 예약을 걸어 두기만 한 시점이나 구간만 바꾼 재설정에서는 상태가 그대로이므로
 * 같은 의미의 이벤트를 다시 보내지 않는다.
 */
@Service
@RequiredArgsConstructor
public class AwayModeService {

    private final SubjectRepository subjects;
    private final RiskAssessmentService assessments;
    private final ApplicationEventPublisher publisher;

    @Transactional
    public void updateAwayMode(
            String subjectAuthSub,
            AwayModeRequest request
    ) {
        if (subjectAuthSub == null || subjectAuthSub.isBlank()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "로그인이 필요합니다.");
        }

        Subject subject = subjects.findByAuthSub(subjectAuthSub)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        "대상자 정보를 찾을 수 없습니다."
                ));

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        boolean wasAway = isAway(subject);
        boolean changed;
        try {
            if (Boolean.TRUE.equals(request.enabled())) {
                subject.scheduleAway(now, request.startsAt(), request.endsAt());
                changed = true;
            } else {
                if (request.hasSchedule()) {
                    throw new IllegalArgumentException(
                            "외출 모드를 해제할 때는 시간을 지정할 수 없습니다."
                    );
                }
                changed = subject.getAwayStartedAt() != null;
                subject.cancelAway(now);
            }
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }

        if (changed) {
            publisher.publishEvent(
                    new SubjectStateChanged(subject.getId(), StateChangeTrigger.AWAY_MODE));
        }

        // 요청을 처리한 지금이 곧 상태가 바뀐 시각이다.
        publishPresenceChange(subject, wasAway, now);

        if (isAway(subject) != wasAway) {
            // 외출 여부가 뒤집히면 부재 기반 지표의 평가 자격이 통째로 바뀐다.
            // 다음 타이머를 기다리지 않고 같은 트랜잭션에서 바로 다시 평가한다.
            assessments.evaluate(
                    subject.getHouseholdId(), now, StateChangeTrigger.AWAY_MODE);
        }
    }

    /**
     * 예약된 외출의 시작·종료를 캐시에 반영한다.
     *
     * <p>이벤트가 들어오지 않아도 담당자 대시보드가 갱신되도록 주기적으로 돈다.
     * 값이 실제로 바뀐 대상자만 실시간 스트림으로 내보낸다.
     */
    @Transactional
    public int applyScheduledTransitions() {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        int changed = 0;

        for (Subject subject : subjects.findAwayTransitionCandidates(now)) {
            boolean wasAway = isAway(subject);
            if (subject.syncMonitoring(now)) {
                publisher.publishEvent(
                        new SubjectStateChanged(subject.getId(), StateChangeTrigger.AWAY_MODE));
                changed++;

                // 발효는 늦어도 상태가 바뀐 시각은 예약된 경계다.
                publishPresenceChange(subject, wasAway, scheduledBoundary(subject, now));

                // 외출이 끝난 가구는 다시 평가 대상이 된다.
                assessments.evaluate(
                        subject.getHouseholdId(), now, StateChangeTrigger.AWAY_MODE);
            }
        }
        return changed;
    }

    /**
     * 외출 여부가 뒤집혔을 때만 AI 분석 서비스로 보낼 이벤트를 남긴다.
     *
     * @param occurredAt 외출 상태가 실제로 바뀐 시각(발행 시각이 아니다)
     */
    private void publishPresenceChange(
            Subject subject,
            boolean wasAway,
            OffsetDateTime occurredAt
    ) {
        boolean nowAway = isAway(subject);
        if (nowAway == wasAway) {
            return;
        }

        publisher.publishEvent(new HouseholdPresenceChanged(
                UUID.randomUUID(),
                subject.getHouseholdId(),
                nowAway ? OutingEventType.OUTING_STARTED : OutingEventType.OUTING_ENDED,
                occurredAt
        ));
    }

    /**
     * 이미 발효된 외출 여부. 구간이 아니라 캐시를 보는 이유는,
     * 아직 스케줄러가 발효시키지 않은 구간까지 바뀐 것으로 세면
     * 시작을 알린 적 없는 가구에 종료만 보내게 되기 때문이다.
     */
    private boolean isAway(Subject subject) {
        return !subject.isMonitoringEnabled();
    }

    /** 예약된 시작·종료 경계. 값이 비어 있으면 발효 시각으로 대신한다. */
    private OffsetDateTime scheduledBoundary(Subject subject, OffsetDateTime now) {
        OffsetDateTime boundary = subject.isMonitoringEnabled()
                ? subject.getAwayUntil()
                : subject.getAwayStartedAt();
        return boundary == null ? now : boundary;
    }
}

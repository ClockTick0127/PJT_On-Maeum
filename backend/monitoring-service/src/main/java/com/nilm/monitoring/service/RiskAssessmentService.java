package com.nilm.monitoring.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nilm.monitoring.config.EventRoutingPolicy;
import com.nilm.monitoring.config.RiskProperties;
import com.nilm.monitoring.config.enums.RiskLevel;
import com.nilm.monitoring.config.enums.StateChangeTrigger;
import com.nilm.monitoring.domain.Notification;
import com.nilm.monitoring.domain.NotificationSetting;
import com.nilm.monitoring.domain.RiskAssessmentRecord;
import com.nilm.monitoring.domain.Subject;
import com.nilm.monitoring.repository.RiskAssessmentRecordRepository;
import com.nilm.monitoring.repository.SubjectRepository;
import com.nilm.monitoring.risk.CurrentState;
import com.nilm.monitoring.risk.RiskAssessment;
import com.nilm.monitoring.risk.RiskAssessor;
import com.nilm.monitoring.risk.RiskPolicy;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * 모니터링 자체 위험 평가의 반영 지점.
 *
 * <p>계산은 {@link RiskAssessor}가 하고, 여기서는 그 결과를 대상자 상태에 옮긴다.
 * 참고 점수를 반영하고, 이벤트 등급의 재발송을 정하고, 평가 이력을 남긴다.
 *
 * <p>자체 평가는 등급을 내지 않는다. 화면에 나가는 등급은 분석 서비스 이벤트가 세운
 * 슬롯에서 온다. 타이머는 그 슬롯의 최대 유지시간과 재발송 간격을 재는 역할만 한다.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class RiskAssessmentService {

    private final SubjectRepository subjects;
    private final HouseholdProfileService profiles;
    private final CurrentStateProvider currentStates;
    private final RiskAssessmentRecordRepository assessments;
    private final NotificationService notifications;
    private final NotificationGate notificationGate;
    private final EventRoutingPolicy routing;
    private final RiskProperties riskProperties;
    private final ApplicationEventPublisher publisher;
    private final ObjectMapper objectMapper;

    private final RiskAssessor assessor = new RiskAssessor();

    /**
     * 가구 한 곳을 평가하고 결과를 반영한다.
     *
     * <p>상태 변경 경로는 자기 트랜잭션 안에서 이 메서드를 부른다.
     * 상태를 바꾼 트랜잭션이 롤백되면 그 상태로 낸 평가도 함께 사라져야 하기 때문이다.
     */
    @Transactional
    public void evaluate(String householdId, OffsetDateTime now, StateChangeTrigger trigger) {
        Subject subject = lockHousehold(householdId);
        if (subject == null) {
            return;
        }
        assess(subject, now, trigger, false);
    }

    /**
     * 타이머가 도는 경로. 한 가구의 실패가 나머지를 막지 않도록 가구마다 트랜잭션을 연다.
     *
     * <p>이벤트 경로와 같은 쓰기 잠금을 상태를 처음 읽는 순간부터 잡는다. 예전에는
     * {@code findById}로 잠금 없이 읽고 나서 점수·등급·{@code lastAlertAt}을 고쳤기 때문에,
     * 타이머와 이벤트가 겹치면 이벤트가 방금 세운 등급이 사라지거나 두 경로가 같은
     * 재발송 시각을 읽어 알림이 둘 나갈 수 있었다. 잠금은 커밋까지 유지되므로
     * 재발송 판정부터 알림 생성과 평가 이력 저장까지가 한 직렬화 구간 안에 들어온다.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void evaluateSubject(
            EvaluationTarget target,
            OffsetDateTime now,
            StateChangeTrigger trigger
    ) {
        // REQUIRES_NEW라 영속성 컨텍스트도 새로 열린다. 이 조회가 그 안의 첫 읽기이자
        // 잠금이므로, 잠그기 전에 읽어 둔 오래된 Subject가 끼어들 자리가 없다.
        Subject subject = lockHousehold(target.householdId());
        if (subject == null) {
            return;
        }
        if (!subject.getId().equals(target.subjectId())) {
            // 목록을 읽은 뒤 가구 구성이 바뀌었다. 다음 순회에서 다시 본다.
            log.warn("가구의 대상자가 순회 시작 이후 바뀌어 평가를 건너뛴다: householdId={}, 기대={}, 실제={}",
                    target.householdId(), target.subjectId(), subject.getId());
            return;
        }

        // 이벤트 등급을 언제까지 붙들고 있을지는 타이머만 판정할 수 있다.
        // 해제 신호(가전 OFF, 담당자 조치)를 놓쳤을 때의 안전장치다.
        boolean released = releaseStaleEventRisk(subject, now);
        // 슬롯을 비운 것만으로도 유효 등급이 내려간다. 평가 결과가 그대로여도 알린다.
        assess(subject, now, trigger, released);
    }

    /**
     * 대상자 id만 아는 호출부를 위한 경로.
     *
     * <p>가구는 엔티티가 아니라 스칼라로 읽는다. 여기서 {@code findById}를 쓰면
     * 잠금 이전 상태의 Subject가 영속성 컨텍스트에 남아, 뒤이어 잠금을 잡아도
     * 그 사이 커밋된 변경이 보이지 않는 1차 캐시 인스턴스가 그대로 쓰인다.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void evaluateSubject(Long subjectId, OffsetDateTime now, StateChangeTrigger trigger) {
        subjects.findHouseholdIdById(subjectId).ifPresent(householdId ->
                evaluateSubject(new EvaluationTarget(subjectId, householdId), now, trigger));
    }

    /** 타이머가 순회할 대상자 목록. 엔티티가 아니라 식별자만 싣는다. */
    @Transactional(readOnly = true)
    public List<EvaluationTarget> targets() {
        return subjects.findAllTargets().stream()
                .map(row -> new EvaluationTarget(row.getId(), row.getHouseholdId()))
                .toList();
    }

    /**
     * 평가할 가구의 대상자를 쓰기 잠금으로 읽는다.
     *
     * @return 대상자가 정확히 한 명이면 그 대상자, 아니면 null
     */
    private Subject lockHousehold(String householdId) {
        List<Subject> matches = subjects.findHouseholdForUpdate(householdId);
        if (matches.size() != 1) {
            log.warn("가구에 대상자가 정확히 한 명이 아니어서 평가를 건너뛴다: householdId={}, 수={}",
                    householdId, matches.size());
            return null;
        }
        return matches.get(0);
    }

    /**
     * 타이머가 한 바퀴 도는 동안 붙들고 다니는 대상.
     *
     * <p>가구를 함께 실어야 타이머가 이벤트 경로와 같은 잠금을 잡을 수 있다.
     * 엔티티를 싣지 않는 것이 요점이다.
     */
    public record EvaluationTarget(Long subjectId, String householdId) {
    }

    /**
     * 이벤트 등급 슬롯을 비운다. 가전의 ON→OFF 전환으로 해제하는 경로다.
     *
     * @return 슬롯이 실제로 비워졌으면 true
     */
    public boolean releaseEventRiskFor(Subject subject, List<String> turnedOff, OffsetDateTime now) {
        String held = subject.getEventRiskAppliance();
        if (held == null || !turnedOff.contains(held)) {
            return false;
        }
        log.info("가전이 꺼져 이벤트 등급을 해제한다: subjectId={}, appliance={}",
                subject.getId(), held);
        return subject.clearEventRisk(now);
    }

    private boolean releaseStaleEventRisk(Subject subject, OffsetDateTime now) {
        OffsetDateTime setAt = subject.getEventRiskSetAt();
        if (setAt == null) {
            return false;
        }
        if (!now.isAfter(setAt.plus(riskProperties.getEventLevelMaxHold()))) {
            return false;
        }
        log.info("최대 유지시간이 지나 이벤트 등급을 해제한다: subjectId={}, setAt={}",
                subject.getId(), setAt);
        return subject.clearEventRisk(now);
    }

    private void assess(
            Subject subject,
            OffsetDateTime now,
            StateChangeTrigger trigger,
            boolean forcePublish
    ) {
        // 예약된 외출의 시작·종료를 먼저 발효시킨다. 스케줄러가 늦게 돌아도
        // 평가가 외출 여부를 잘못 보지 않게 한다.
        subject.syncMonitoring(now);

        RiskPolicy policy = riskProperties.toPolicy();
        CurrentState state =
                currentStates.of(subject.getHouseholdId(), subject.isAwayAt(now), now);
        RiskAssessment assessment = assessor.assess(
                profiles.resolveActive(subject.getHouseholdId(), now), state, policy);

        Subject.AssessmentOutcome outcome = subject.applyAssessment(assessment, now);

        UUID assessmentId = UUID.randomUUID();
        Notification notification = alertIfNeeded(subject, assessmentId, outcome, state, now);

        saveHistory(subject, assessmentId, assessment, outcome, state, trigger, notification, now);

        // 같은 결과가 반복되는 동안에는 화면을 건드리지 않는다.
        // 타이머가 매분 도는데 매번 내보내면 담당자 대시보드가 빈 갱신으로 가득 찬다.
        if (forcePublish || outcome.changed() || notification != null) {
            publisher.publishEvent(
                    new SubjectStateChanged(subject.getId(), StateChangeTrigger.ASSESSMENT));
        }
    }

    /**
     * 알림을 다시 낼지 정한다.
     *
     * <p>유효 등급은 이벤트 슬롯에서 오고, 첫 알림은 이벤트 경로가 이미 냈다. 여기서는
     * 같은 주의·위험 등급이 이어지는 동안 재발송 간격이 지났을 때만 다시 낸다.
     * 같은 판단으로 같은 알림을 반복하지 않기 위해서다.
     */
    private Notification alertIfNeeded(
            Subject subject,
            UUID assessmentId,
            Subject.AssessmentOutcome outcome,
            CurrentState state,
            OffsetDateTime now
    ) {
        RiskLevel level = subject.effectiveRiskLevel();
        if (level == RiskLevel.NORMAL) {
            return null;
        }
        if (state.away()) {
            // 외출 중에는 다시 알리지 않는다. 첫 알림의 억제 규칙은 이벤트 경로가 따로 정한다.
            return null;
        }

        boolean realertDue = subject.getLastAlertAt() == null
                || !now.isBefore(subject.getLastAlertAt().plus(riskProperties.getRealertInterval()));
        if (!outcome.raised() && !realertDue) {
            return null;
        }
        if (subject.getAuthSub() == null || subject.getAuthSub().isBlank()) {
            log.warn("평가 완료, 수신자 식별자 없음: subjectId={}", subject.getId());
            return null;
        }

        Notification notification = notifications.createNotification(
                null,
                assessmentId,
                subject.getId(),
                subject.getAuthSub(),
                true,
                routing.responseDeadline()
        );
        subject.markAlerted(now);

        // 설정이 꺼져 있어도 행은 남긴다. 막는 것은 발송뿐이다.
        if (notificationGate.allows(subject, level, NotificationSetting.Channel.PUSH)) {
            log.info("자체 평가 알림 생성, 웹푸시 발송 요청: notificationId={}, subjectId={}, level={}, "
                            + "등급상승={}",
                    notification.getId(), subject.getId(), level, outcome.raised());
            publisher.publishEvent(new NotificationReady(
                    notification.getId(),
                    "안전 확인 요청",
                    "생활 패턴이 평소와 달라 안전을 확인하고 있습니다. "
                            + NotificationReady.RESPONSE_QUESTION
            ));
        } else {
            log.info("담당자 수신 설정이 꺼져 있어 발송하지 않는다: subjectId={}, level={}",
                    subject.getId(), level);
        }
        return notification;
    }

    /**
     * 평가 이력을 남긴다.
     *
     * <p>매분 도는 타이머의 결과를 모두 쌓으면 이력이 의미 없이 불어난다.
     * 등급이 바뀌었거나, 알림을 냈거나, 평가 가능 여부가 달라진 순간은 언제나 남기고,
     * 같은 결과가 이어지는 동안에는 설정된 간격으로만 남긴다.
     */
    private void saveHistory(
            Subject subject,
            UUID assessmentId,
            RiskAssessment assessment,
            Subject.AssessmentOutcome outcome,
            CurrentState state,
            StateChangeTrigger trigger,
            Notification notification,
            OffsetDateTime now
    ) {
        Optional<RiskAssessmentRecord> last = assessments.findLatestBySubjectId(subject.getId());
        boolean statusChanged = last
                .map(record -> record.getAssessmentStatus() != assessment.status())
                .orElse(true);
        boolean intervalElapsed = last
                .map(record -> !now.isBefore(
                        record.getAssessedAt().plus(riskProperties.getAssessmentLogInterval())))
                .orElse(true);

        if (!outcome.levelChanged() && notification == null && !statusChanged && !intervalElapsed) {
            return;
        }

        RiskAssessmentRecord record = new RiskAssessmentRecord(
                assessmentId,
                subject.getId(),
                subject.getHouseholdId(),
                now,
                state.lastObservedAt(),
                trigger,
                assessment.score(),
                assessment.level(),
                assessment.status(),
                assessment.confidence(),
                indicatorsJson(assessment),
                assessment.profileVersion(),
                assessment.policyVersion(),
                assessment.scoreVersion(),
                outcome.levelChanged(),
                now
        );
        if (notification != null) {
            record.linkNotification(notification.getId());
        }
        assessments.save(record);
    }

    private String indicatorsJson(RiskAssessment assessment) {
        try {
            return objectMapper.writeValueAsString(assessment.indicators());
        } catch (Exception e) {
            // 이력 직렬화 실패가 평가 반영을 되돌릴 이유는 되지 않는다.
            log.warn("평가 근거를 JSON으로 남기지 못했습니다: status={}", assessment.status(), e);
            return null;
        }
    }
}

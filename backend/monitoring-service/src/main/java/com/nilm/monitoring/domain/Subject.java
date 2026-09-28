package com.nilm.monitoring.domain;

import com.nilm.monitoring.config.enums.RiskLevel;
import com.nilm.monitoring.risk.AssessmentStatus;
import com.nilm.monitoring.risk.RiskAssessment;
import jakarta.persistence.*;
import lombok.Getter;

import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.UUID;

@Entity
@Getter
@Table(name = "subjects")
public class Subject {

    /** 요청이 서버에 닿기까지의 시계 차이를 허용하는 폭. */
    private static final Duration START_TOLERANCE = Duration.ofMinutes(1);

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "household_id", nullable = false)
    private String householdId;

    @Column(name = "auth_sub", unique = true)
    private String authSub;

    @Column(name = "birth_date", nullable = false)
    private LocalDate birthDate;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private String phone;

    @Column(nullable = false)
    private String address;

    @Column(name = "risk_policy_id")
    private Long riskPolicyId; // 위험 정책 ID

    @Column(name = "monitoring_enabled", nullable = false)
    private boolean monitoringEnabled = true; // 모니터링 활성화 여부

    @Column(name = "away_started_at")
    private OffsetDateTime awayStartedAt; // 외출 시작 시간

    @Column(name = "away_until")
    private OffsetDateTime awayUntil; // 외출 종료 시간

    @Column(name = "manager_memo", columnDefinition = "text")
    private String managerMemo; // 담당자 메모

    @Column(name = "manager_id")
    private Long managerId; // 배정 담당자 ID, 대상자 1명 -> 담당자 1명만 배정

    @Column(name = "address_detail", length = 100)
    private String addressDetail;

    @Column(name = "state_version", nullable = false)
    private long stateVersion = 1;

    /**
     * 화면과 스트림에 나가는 유효 등급.
     * 자체 평가 등급과 이벤트 등급 중 높은 쪽이며, 직접 쓰지 않고
     * {@link #refreshEffectiveRisk(OffsetDateTime)}가 두 슬롯에서 다시 계산한다.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "current_risk_level", nullable = false)
    private RiskLevel currentRiskLevel = RiskLevel.NORMAL;

    /** 유효 등급을 만든 쪽의 점수. */
    @Column(name = "current_risk_score", nullable = false)
    private int currentRiskScore;

    /**
     * v1 점수식이 스스로 계산하던 등급. v2부터 자체 평가는 등급을 내지 않으므로
     * 다음 평가가 남은 값을 지운다. 컬럼과 이력 호환을 위해 필드는 남겨 둔다.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "assessed_risk_level")
    private RiskLevel assessedRiskLevel;

    @Column(name = "assessed_risk_score")
    private Integer assessedRiskScore;

    @Enumerated(EnumType.STRING)
    @Column(name = "assessment_status")
    private AssessmentStatus assessmentStatus;

    @Column(name = "assessment_confidence")
    private Double assessmentConfidence;

    @Column(name = "assessed_at")
    private OffsetDateTime assessedAt;

    /** 마지막으로 VALID였던 평가 시각. 평가 불가가 이어져도 지우지 않는다. */
    @Column(name = "last_valid_assessed_at")
    private OffsetDateTime lastValidAssessedAt;

    /** v1 자체 평가 등급이 확정된 시각. v2에서는 더 쓰지 않는다. */
    @Column(name = "risk_level_since")
    private OffsetDateTime riskLevelSince;

    /** v1 히스테리시스 후보 등급. v2에서는 쓰지 않고 다음 평가가 지운다. */
    @Enumerated(EnumType.STRING)
    @Column(name = "pending_risk_level")
    private RiskLevel pendingRiskLevel;

    @Column(name = "pending_since")
    private OffsetDateTime pendingSince;

    /** 분석 서비스 이벤트가 세운 등급. 자체 평가가 덮어쓰지 못하는 별도 슬롯이다. */
    @Enumerated(EnumType.STRING)
    @Column(name = "event_risk_level")
    private RiskLevel eventRiskLevel;

    @Column(name = "event_risk_score")
    private Integer eventRiskScore;

    @Column(name = "event_risk_event_id")
    private UUID eventRiskEventId;

    /** 이 가전의 ON→OFF 전환을 보면 슬롯을 해제한다. */
    @Column(name = "event_risk_appliance", length = 50)
    private String eventRiskAppliance;

    @Column(name = "event_risk_set_at")
    private OffsetDateTime eventRiskSetAt;

    /** 같은 등급이 이어질 때 알림을 다시 보낼지 판정하는 기준. */
    @Column(name = "last_alert_at")
    private OffsetDateTime lastAlertAt;

    @Column(name = "last_activity_at")
    private OffsetDateTime lastActivityAt;

    @Column(name = "last_activity_appliance")
    private String lastActivityAppliance;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt = OffsetDateTime.now(ZoneOffset.UTC);

    protected Subject() {
    }

    public Subject(
            String householdId,
            String name,
            LocalDate birthDate,
            String phone,
            String address,
            String addressDetail,
            String managerMemo,
            Long managerId
    ) {
        if (managerId == null) {
            throw new IllegalArgumentException(
                    "대상자를 등록할 담당자가 필요합니다."
            );
        }

        this.householdId = householdId;
        this.name = name;
        this.birthDate = birthDate;
        this.phone = phone;
        this.address = address;
        this.addressDetail = addressDetail;
        this.managerMemo = managerMemo;

        // 로그인한 담당자의 DB ID를 배정한다.
        this.managerId = managerId;

        this.monitoringEnabled = true;
        this.stateVersion = 1;
        this.currentRiskLevel = RiskLevel.NORMAL;
        this.currentRiskScore = 0;
        this.updatedAt = OffsetDateTime.now(ZoneOffset.UTC);

        // authSub에는 값을 넣지 않는다.
        // 대상자 본인의 로그인 계정 연결 시 별도로 설정한다.
    }

    public void assignManager(Long managerId) {
        this.managerId = managerId;
    }

    /**
     * 분석 서비스 이벤트가 정한 등급을 이벤트 슬롯에 세운다.
     *
     * <p>자체 평가 슬롯은 건드리지 않는다. 설계 11.4절대로 두 판단을 따로 보관하고
     * 화면에는 둘 중 높은 값만 내보내, 타이머 평가가 낮은 점수를 내도
     * 이벤트 등급이 지워지지 않게 한다.
     *
     * @param representativeScore 이벤트 계약에 score가 없으므로 등급의 대표값을 쓴다
     * @return 유효 등급이 실제로 바뀌었으면 true
     */
    public boolean applyEventRisk(
            RiskLevel level,
            int representativeScore,
            UUID eventId,
            String applianceType,
            OffsetDateTime now
    ) {
        this.eventRiskLevel = level;
        this.eventRiskScore = representativeScore;
        this.eventRiskEventId = eventId;
        this.eventRiskAppliance = applianceType;
        this.eventRiskSetAt = now;
        return refreshEffectiveRisk(now);
    }

    /**
     * 이벤트 등급 슬롯을 비운다.
     *
     * <p>해제 조건은 설계 11.4절의 세 가지다: 해당 가전의 ON→OFF 전환,
     * 담당자의 조치 완료, 안전장치로서의 최대 유지시간 경과.
     *
     * @return 슬롯이 실제로 비워졌으면 true
     */
    public boolean clearEventRisk(OffsetDateTime now) {
        if (eventRiskLevel == null) {
            return false;
        }
        this.eventRiskLevel = null;
        this.eventRiskScore = null;
        this.eventRiskEventId = null;
        this.eventRiskAppliance = null;
        this.eventRiskSetAt = null;
        refreshEffectiveRisk(now);
        return true;
    }

    /**
     * 자체 평가 결과를 반영한다.
     *
     * <p>자체 평가는 참고 점수만 낸다. 등급은 분석 서비스 이벤트가 세운 슬롯이 정한다.
     * v1 점수식이 세워 둔 등급이나 후보가 남아 있으면 여기서 지운다. 남겨 두면 판단 주체가
     * 사라진 등급이 해제 경로 없이 화면에 계속 걸린다.
     *
     * <p>VALID가 아니면 점수를 건드리지 않는다. 평가 불가를 0점으로 덮어쓰면
     * 화면에서 "데이터 없음"과 "평소와 같음"을 구분할 수 없게 된다(설계 11.1절).
     */
    public AssessmentOutcome applyAssessment(RiskAssessment assessment, OffsetDateTime now) {
        RiskLevel before = effectiveRiskLevel();
        AssessmentStatus previousStatus = assessmentStatus;
        Integer previousScore = assessedRiskScore;

        this.assessmentStatus = assessment.status();
        this.assessmentConfidence = assessment.confidence();
        this.assessedAt = now;
        this.assessedRiskLevel = null;
        this.pendingRiskLevel = null;
        this.pendingSince = null;

        if (assessment.status() == AssessmentStatus.VALID && assessment.score() != null) {
            this.lastValidAssessedAt = now;
            this.assessedRiskScore = assessment.score();
        }

        RiskLevel after = effectiveRiskLevel();
        int score = effectiveRiskScore();
        boolean effectiveChanged = after != currentRiskLevel || score != currentRiskScore;
        this.currentRiskLevel = after;
        this.currentRiskScore = score;

        // 타이머가 매분 도는데 결과가 같을 때마다 상태 버전을 올리면
        // 담당자 화면이 아무 일도 없는 갱신으로 가득 찬다.
        boolean changed = effectiveChanged
                || previousStatus != assessment.status()
                || !Objects.equals(previousScore, assessedRiskScore);
        if (changed) {
            touch(now);
        }
        return new AssessmentOutcome(before != after, changed, before, after);
    }

    /** 알림을 실제로 만든 시각. 같은 등급의 재발송 간격을 여기서 잰다. */
    public void markAlerted(OffsetDateTime now) {
        this.lastAlertAt = now;
    }

    /** 화면·스트림에 내보내는 유효 등급 = max(자체 평가, 이벤트). */
    public RiskLevel effectiveRiskLevel() {
        if (assessedRiskLevel == null) {
            return eventRiskLevel == null ? RiskLevel.NORMAL : eventRiskLevel;
        }
        if (eventRiskLevel == null) {
            return assessedRiskLevel;
        }
        return assessedRiskLevel.compareTo(eventRiskLevel) >= 0 ? assessedRiskLevel : eventRiskLevel;
    }

    /** 유효 등급을 만든 쪽. 담당자 화면이 "왜 이 등급인가"를 구분하는 데 쓴다. */
    public String riskSource() {
        RiskLevel effective = effectiveRiskLevel();
        if (effective == RiskLevel.NORMAL && assessedRiskLevel == null && eventRiskLevel == null) {
            return "NONE";
        }
        if (eventRiskLevel != null && eventRiskLevel == effective) {
            // 두 슬롯이 같은 등급이면 즉시 알림 경로를 출처로 본다.
            return "EVENT";
        }
        return assessedRiskLevel == null ? "NONE" : "ASSESSMENT";
    }

    /**
     * 두 슬롯에서 유효 등급과 점수를 다시 계산한다.
     *
     * @return 등급 또는 점수가 실제로 바뀌었으면 true
     */
    public boolean refreshEffectiveRisk(OffsetDateTime now) {
        RiskLevel level = effectiveRiskLevel();
        int score = effectiveRiskScore();

        if (level == currentRiskLevel && score == currentRiskScore) {
            return false;
        }
        this.currentRiskLevel = level;
        this.currentRiskScore = score;
        touch(now);
        return true;
    }

    /** 유효 등급을 만든 쪽의 점수. */
    private int effectiveRiskScore() {
        if ("EVENT".equals(riskSource())) {
            return eventRiskScore == null ? 0 : eventRiskScore;
        }
        return assessedRiskScore == null ? 0 : assessedRiskScore;
    }

    /**
     * 평가 반영 결과.
     *
     * @param levelChanged 유효 등급이 실제로 움직였는지
     * @param changed 등급·점수·평가 상태 중 하나라도 달라졌는지.
     *                같은 결과가 반복되는 동안에는 화면을 갱신하지 않는 기준이다
     */
    public record AssessmentOutcome(
            boolean levelChanged,
            boolean changed,
            RiskLevel before,
            RiskLevel after
    ) {

        /** 등급이 올라갔는지. 알림은 상승에만 건다. */
        public boolean raised() {
            return after.compareTo(before) > 0;
        }
    }

    /**
     * 가전이 ON에서 OFF로 넘어간 시각을 마지막 활동으로 남긴다.
     * 스냅샷이 순서가 뒤바뀌어 도착해도 마지막 활동이 과거로 되돌아가지 않게 막는다.
     *
     * @return 값이 실제로 갱신되었으면 true
     */
    public boolean recordActivity(
            String applianceType,
            OffsetDateTime endedAt,
            OffsetDateTime updatedAt
    ) {
        if (applianceType == null || applianceType.isBlank() || endedAt == null) {
            return false;
        }
        if (lastActivityAt != null && endedAt.isBefore(lastActivityAt)) {
            return false;
        }
        this.lastActivityAt = endedAt;
        this.lastActivityAppliance = applianceType;
        touch(updatedAt);
        return true;
    }

    public void touch(OffsetDateTime updatedAt) {
        this.stateVersion++;
        this.updatedAt = updatedAt;
    }

    /**
     * 주어진 시각이 외출 구간 안인지 판정한다.
     *
     * <p>{@code awayUntil}이 없으면 해제할 때까지 이어지는 무기한 외출이다.
     * 시각으로 판정하므로 이벤트가 늦게 도착해도 같은 답이 나온다.
     */
    public boolean isAwayAt(OffsetDateTime at) {
        if (awayStartedAt == null || at.isBefore(awayStartedAt)) {
            return false;
        }
        return awayUntil == null || at.isBefore(awayUntil);
    }

    /** 아직 시작하지 않은 외출 예약이 걸려 있는지. */
    public boolean hasScheduledAway(OffsetDateTime at) {
        return awayStartedAt != null && at.isBefore(awayStartedAt);
    }

    /**
     * 외출을 설정한다. {@code startsAt}이 없으면 즉시 시작하고,
     * {@code endsAt}이 없으면 해제할 때까지 이어진다.
     *
     * <p>구간은 한 벌만 보관하므로 새로 설정하면 이전 외출 기록을 덮어쓴다.
     */
    public void scheduleAway(
            OffsetDateTime now,
            OffsetDateTime startsAt,
            OffsetDateTime endsAt
    ) {
        if (startsAt != null && startsAt.isBefore(now.minus(START_TOLERANCE))) {
            throw new IllegalArgumentException(
                    "외출 시작 시간은 현재 시각 이후여야 합니다."
            );
        }

        OffsetDateTime start = startsAt == null ? now : startsAt;
        if (endsAt != null && !endsAt.isAfter(start)) {
            throw new IllegalArgumentException(
                    "외출 종료 시간은 시작 시간 이후여야 합니다."
            );
        }

        this.awayStartedAt = start;
        this.awayUntil = endsAt;
        syncMonitoring(now);
    }

    /**
     * 외출을 해제한다. 진행 중이면 지금 끊되 구간은 남겨 둔다.
     * 늦게 도착한 이벤트가 외출 중 발생이었는지 판정할 수 있어야 하기 때문이다.
     */
    public void cancelAway(OffsetDateTime now) {
        if (awayStartedAt == null) {
            return;
        }

        if (isAwayAt(now)) {
            this.awayUntil = now;
        } else if (now.isBefore(awayStartedAt)) {
            // 아직 시작하지 않은 예약은 흔적 없이 취소한다.
            this.awayStartedAt = null;
            this.awayUntil = null;
        }
        syncMonitoring(now);
    }

    /**
     * 외출 구간에서 계산한 값으로 모니터링 플래그를 맞춘다.
     *
     * <p>이 플래그는 목록 조회와 실시간 전송을 위한 캐시일 뿐이고,
     * 판단의 근거는 언제나 {@link #isAwayAt(OffsetDateTime)}이다.
     *
     * @return 값이 실제로 바뀌었으면 true
     */
    public boolean syncMonitoring(OffsetDateTime now) {
        boolean next = !isAwayAt(now);
        if (next == monitoringEnabled) {
            return false;
        }

        this.monitoringEnabled = next;
        touch(now);
        return true;
    }
}

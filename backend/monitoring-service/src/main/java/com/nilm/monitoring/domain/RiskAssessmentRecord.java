package com.nilm.monitoring.domain;

import com.nilm.monitoring.config.enums.RiskLevel;
import com.nilm.monitoring.config.enums.StateChangeTrigger;
import com.nilm.monitoring.risk.AssessmentStatus;
import jakarta.persistence.*;
import java.time.OffsetDateTime;
import java.util.UUID;
import lombok.Getter;

/**
 * 모니터링 자체 평가 1회의 기록(설계 12장).
 *
 * <p>점수만 남기면 나중에 왜 그 등급이었는지 되짚을 수 없다. 어떤 프로필·정책·점수식으로
 * 계산했는지와 지표별 관측값·비교값을 함께 보관해, 정책을 바꾼 뒤의 재평가 결과와
 * 당시 실제 발송 이력을 구분할 수 있게 한다.
 *
 * <p>점수·등급은 nullable이다. 평가 불가를 0점·정상으로 적지 않는다.
 */
@Entity
@Getter
@Table(name = "risk_assessments")
public class RiskAssessmentRecord {

    @Id
    private UUID id;

    @Column(name = "subject_id", nullable = false)
    private Long subjectId;

    @Column(name = "household_id", nullable = false)
    private String householdId;

    @Column(name = "assessed_at", nullable = false)
    private OffsetDateTime assessedAt;

    /** 평가가 근거로 삼은 현재 입력의 기준 시각(마지막 관측). */
    @Column(name = "data_as_of")
    private OffsetDateTime dataAsOf;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private StateChangeTrigger trigger;

    @Column(name = "risk_score")
    private Integer riskScore;

    @Enumerated(EnumType.STRING)
    @Column(name = "risk_level", length = 20)
    private RiskLevel riskLevel;

    @Enumerated(EnumType.STRING)
    @Column(name = "assessment_status", nullable = false, length = 30)
    private AssessmentStatus assessmentStatus;

    @Column(nullable = false)
    private double confidence;

    /** 지표별 계산 결과를 담은 JSON. H2에서 jsonb를 쓸 수 없어 문자열로 둔다. */
    @Column(columnDefinition = "text")
    private String indicators;

    @Column(name = "profile_version", length = 100)
    private String profileVersion;

    @Column(name = "policy_version", nullable = false, length = 50)
    private String policyVersion;

    @Column(name = "score_version", nullable = false, length = 50)
    private String scoreVersion;

    @Column(name = "level_changed", nullable = false)
    private boolean levelChanged;

    @Column(name = "notification_id")
    private Long notificationId;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    protected RiskAssessmentRecord() {
    }

    public RiskAssessmentRecord(
            UUID id,
            Long subjectId,
            String householdId,
            OffsetDateTime assessedAt,
            OffsetDateTime dataAsOf,
            StateChangeTrigger trigger,
            Integer riskScore,
            RiskLevel riskLevel,
            AssessmentStatus assessmentStatus,
            double confidence,
            String indicators,
            String profileVersion,
            String policyVersion,
            String scoreVersion,
            boolean levelChanged,
            OffsetDateTime createdAt
    ) {
        this.id = id;
        this.subjectId = subjectId;
        this.householdId = householdId;
        this.assessedAt = assessedAt;
        this.dataAsOf = dataAsOf;
        this.trigger = trigger;
        this.riskScore = riskScore;
        this.riskLevel = riskLevel;
        this.assessmentStatus = assessmentStatus;
        this.confidence = confidence;
        this.indicators = indicators;
        this.profileVersion = profileVersion;
        this.policyVersion = policyVersion;
        this.scoreVersion = scoreVersion;
        this.levelChanged = levelChanged;
        this.createdAt = createdAt;
    }

    /** 이 평가가 만든 알림을 이어 붙인다. */
    public void linkNotification(Long notificationId) {
        this.notificationId = notificationId;
    }
}

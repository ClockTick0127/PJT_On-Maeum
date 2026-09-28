package com.nilm.monitoring.domain;

import com.nilm.monitoring.config.enums.RiskLevel;
import jakarta.persistence.*;
import lombok.Getter;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Getter
@Table(name = "analysis_events")
public class AnalysisEvent {

    @Id
    private UUID id;

    @Column(name = "subject_id", nullable = false)
    private Long subjectId;

    @Column(name = "household_id", nullable = false)
    private String householdId;

    @Column(name = "event_type")
    private String eventType;

    @Column(name = "appliance_type")
    private String applianceType;

    @Column(name = "risk_score", nullable = false)
    private int riskScore;

    @Enumerated(EnumType.STRING)
    @Column(name = "risk_level", nullable = false)
    private RiskLevel riskLevel;

    @Column(name = "occurred_at", nullable = false)
    private OffsetDateTime occurredAt;

    @Column(nullable = false, columnDefinition = "text")
    private String reason;

    @Column(name = "risk_policy_id")
    private Long riskPolicyId;

    protected AnalysisEvent() {
    }

    public AnalysisEvent(UUID id, Long subjectId, String householdId,
                         String eventType, String applianceType, int riskScore,
                         RiskLevel riskLevel, OffsetDateTime occurredAt,
                         String reason, Long riskPolicyId) {
        this.id = id;
        this.subjectId = subjectId;
        this.householdId = householdId;
        this.eventType = eventType;
        this.applianceType = applianceType;
        this.riskScore = riskScore;
        this.riskLevel = riskLevel;
        this.occurredAt = occurredAt;
        this.reason = reason;
        this.riskPolicyId = riskPolicyId;
    }
}

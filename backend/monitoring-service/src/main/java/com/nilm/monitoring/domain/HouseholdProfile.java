package com.nilm.monitoring.domain;

import jakarta.persistence.*;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import lombok.Getter;

/**
 * Gold 배치가 만든 가구 프로필 한 버전의 머리말.
 *
 * <p>가구당 ACTIVE는 최대 한 행이다. 부분 유니크 인덱스는 H2에서 쓸 수 없어
 * 수신 서비스가 가구 행 락을 잡은 한 트랜잭션 안에서 이 불변식을 지킨다.
 */
@Entity
@Getter
@Table(name = "household_profiles")
public class HouseholdProfile {

    public enum Status {

        /** 평가에 쓰는 최신 버전 */
        ACTIVE,

        /** 더 새로운 버전에 밀렸거나, 늦게 도착한 구버전 */
        SUPERSEDED,

        /** 품질이 모자라 평가에 쓰지 않기로 한 버전 */
        REJECTED,

        /** 발효 시각 전까지 현재 ACTIVE를 보존하는 미래 후보 */
        PENDING,

        /** 비교 관측 전용. 운영 프로필로 자동 승격하지 않는다. */
        SHADOW
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "household_id", nullable = false)
    private String householdId;

    @Column(name = "profile_version", nullable = false, length = 100)
    private String profileVersion;

    @Column(name = "profile_revision", nullable = false)
    private long profileRevision;

    @Column(name = "delivery_mode", nullable = false, length = 20)
    private String deliveryMode;

    @Column(name = "as_of_date", nullable = false)
    private LocalDate asOfDate;

    @Column(name = "window_start_date")
    private LocalDate windowStartDate;

    @Column(name = "window_end_date")
    private LocalDate windowEndDate;

    @Column(name = "effective_from", nullable = false)
    private OffsetDateTime effectiveFrom;

    @Column(name = "published_at")
    private OffsetDateTime publishedAt;

    @Column(name = "received_at", nullable = false)
    private OffsetDateTime receivedAt;

    @Column(name = "input_snapshot_id", length = 100)
    private String inputSnapshotId;

    @Column(name = "rule_version", length = 50)
    private String ruleVersion;

    @Column(name = "statistic_rule_version", length = 50)
    private String statisticRuleVersion;

    @Column(name = "quality_status", nullable = false, length = 30)
    private String qualityStatus;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private Status status;

    @Column(name = "rejection_reason", length = 255)
    private String rejectionReason;

    protected HouseholdProfile() {
    }

    public HouseholdProfile(
            String householdId,
            String profileVersion,
            long profileRevision,
            String deliveryMode,
            LocalDate asOfDate,
            LocalDate windowStartDate,
            LocalDate windowEndDate,
            OffsetDateTime effectiveFrom,
            OffsetDateTime publishedAt,
            OffsetDateTime receivedAt,
            String inputSnapshotId,
            String ruleVersion,
            String statisticRuleVersion,
            String qualityStatus,
            Status status,
            String rejectionReason
    ) {
        this.householdId = householdId;
        this.profileVersion = profileVersion;
        this.profileRevision = profileRevision;
        this.deliveryMode = deliveryMode;
        this.asOfDate = asOfDate;
        this.windowStartDate = windowStartDate;
        this.windowEndDate = windowEndDate;
        this.effectiveFrom = effectiveFrom;
        this.publishedAt = publishedAt;
        this.receivedAt = receivedAt;
        this.inputSnapshotId = inputSnapshotId;
        this.ruleVersion = ruleVersion;
        this.statisticRuleVersion = statisticRuleVersion;
        this.qualityStatus = qualityStatus;
        this.status = status;
        this.rejectionReason = rejectionReason;
    }

    /** 더 새로운 버전이 자리를 넘겨받았다. 행은 이력으로 남긴다. */
    public void supersede() {
        this.status = Status.SUPERSEDED;
    }
}

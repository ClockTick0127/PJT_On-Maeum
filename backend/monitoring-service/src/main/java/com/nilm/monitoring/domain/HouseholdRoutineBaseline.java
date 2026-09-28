package com.nilm.monitoring.domain;

import jakarta.persistence.*;
import java.math.BigDecimal;
import lombok.Getter;

/**
 * 프로필 한 버전에 속한 가전별 생활 리듬 기준선.
 * baseline_scope가 OVERALL이면 전체 기간, WEEKDAY이면 해당 요일 하루치다.
 */
@Entity
@Getter
@Table(name = "household_routine_baselines")
public class HouseholdRoutineBaseline {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "profile_id", nullable = false)
    private Long profileId;

    @Column(name = "appliance_type", nullable = false, length = 50)
    private String applianceType;

    @Column(name = "baseline_scope", nullable = false, length = 20)
    private String baselineScope;

    @Column(length = 3)
    private String weekday;

    @Column(name = "sample_days", nullable = false)
    private int sampleDays;

    @Column(name = "active_days", nullable = false)
    private int activeDays;

    @Column(name = "daily_use_probability", precision = 5, scale = 4)
    private BigDecimal dailyUseProbability;

    @Column(name = "reliability_weight", precision = 5, scale = 4)
    private BigDecimal reliabilityWeight;

    @Column(name = "first_use_time_p50_second")
    private Integer firstUseTimeP50Second;

    @Column(name = "expected_until_second")
    private Integer expectedUntilSecond;

    @Column(name = "preferred_window_start_second")
    private Integer preferredWindowStartSecond;

    @Column(name = "preferred_window_end_second")
    private Integer preferredWindowEndSecond;

    @Column(name = "quality_status", nullable = false, length = 30)
    private String qualityStatus;

    /** 배치가 판정한 사용 가능 여부. 평가에서 쓸지 말지를 이 값으로 가른다. */
    @Column(nullable = false)
    private boolean enabled;

    protected HouseholdRoutineBaseline() {
    }

    public HouseholdRoutineBaseline(
            Long profileId,
            String applianceType,
            String baselineScope,
            String weekday,
            int sampleDays,
            int activeDays,
            BigDecimal dailyUseProbability,
            BigDecimal reliabilityWeight,
            Integer firstUseTimeP50Second,
            Integer expectedUntilSecond,
            Integer preferredWindowStartSecond,
            Integer preferredWindowEndSecond,
            String qualityStatus,
            boolean enabled
    ) {
        this.profileId = profileId;
        this.applianceType = applianceType;
        this.baselineScope = baselineScope;
        this.weekday = weekday;
        this.sampleDays = sampleDays;
        this.activeDays = activeDays;
        this.dailyUseProbability = dailyUseProbability;
        this.reliabilityWeight = reliabilityWeight;
        this.firstUseTimeP50Second = firstUseTimeP50Second;
        this.expectedUntilSecond = expectedUntilSecond;
        this.preferredWindowStartSecond = preferredWindowStartSecond;
        this.preferredWindowEndSecond = preferredWindowEndSecond;
        this.qualityStatus = qualityStatus;
        this.enabled = enabled;
    }
}

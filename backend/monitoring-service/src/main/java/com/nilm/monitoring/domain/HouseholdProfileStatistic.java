package com.nilm.monitoring.domain;

import jakarta.persistence.*;
import lombok.Getter;

/**
 * 프로필 한 버전에 속한 시간대별 통계 한 줄.
 * 지표마다 단위와 채워지는 열이 달라 값 열(p50·p90·mad)을 공통으로 둔다.
 */
@Entity
@Getter
@Table(name = "household_profile_statistics")
public class HouseholdProfileStatistic {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "profile_id", nullable = false)
    private Long profileId;

    @Column(name = "metric_name", nullable = false, length = 50)
    private String metricName;

    /** 가구 전체 지표면 null. */
    @Column(name = "appliance_type", length = 50)
    private String applianceType;

    @Column(name = "weekday_group", nullable = false, length = 10)
    private String weekdayGroup;

    /** 시간대를 나누지 않는 지표면 null. */
    @Column(name = "time_bucket", length = 20)
    private String timeBucket;

    @Column(name = "sample_count", nullable = false)
    private long sampleCount;

    @Column(name = "eligible_day_count", nullable = false)
    private long eligibleDayCount;

    private Double p50;

    private Double p90;

    private Double mad;

    @Column(length = 20)
    private String unit;

    @Column(name = "quality_status", nullable = false, length = 30)
    private String qualityStatus;

    protected HouseholdProfileStatistic() {
    }

    public HouseholdProfileStatistic(
            Long profileId,
            String metricName,
            String applianceType,
            String weekdayGroup,
            String timeBucket,
            long sampleCount,
            long eligibleDayCount,
            Double p50,
            Double p90,
            Double mad,
            String unit,
            String qualityStatus
    ) {
        this.profileId = profileId;
        this.metricName = metricName;
        this.applianceType = applianceType;
        this.weekdayGroup = weekdayGroup;
        this.timeBucket = timeBucket;
        this.sampleCount = sampleCount;
        this.eligibleDayCount = eligibleDayCount;
        this.p50 = p50;
        this.p90 = p90;
        this.mad = mad;
        this.unit = unit;
        this.qualityStatus = qualityStatus;
    }
}

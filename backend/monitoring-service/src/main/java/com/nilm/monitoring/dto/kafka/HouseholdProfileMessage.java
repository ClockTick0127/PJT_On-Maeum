package com.nilm.monitoring.dto.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;

/**
 * gold.household-profile.v1 메시지. Key는 household_id이고 가구·버전당 한 건이다.
 *
 * <p>필드 이름은 Gold 배치의 산출 컬럼명을 그대로 따른다.
 * 배치가 열을 추가해도 소비가 멈추지 않도록 모르는 필드는 무시한다.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record HouseholdProfileMessage(

        @JsonProperty("schema_version")
        Integer schemaVersion,

        @JsonProperty("household_id")
        String householdId,

        /**
         * Gold 배치의 run_id. 멱등 판정의 기준이다.
         * SHADOW -> ACTIVE 전환은 새 실행이므로 새 profile_version을 사용한다.
         */
        @JsonProperty("profile_version")
        String profileVersion,

        /**
         * 같은 as_of_date 안에서 생산자가 단조 증가시키는 실행 순번.
         * 수신 순서가 바뀌어도 더 낮은 revision은 운영 ACTIVE를 덮지 못한다.
         */
        @JsonProperty("profile_revision")
        Long profileRevision,

        /** ACTIVE만 운영 후보가 된다. SHADOW는 비교용 이력으로만 저장한다. */
        @JsonProperty("delivery_mode")
        String deliveryMode,

        /** 프로필이 담고 있는 마지막 관측일. 평가 자격을 따지는 기준이다. */
        @JsonProperty("as_of_date")
        LocalDate asOfDate,

        @JsonProperty("window_start_date")
        LocalDate windowStartDate,

        @JsonProperty("window_end_date")
        LocalDate windowEndDate,

        @JsonProperty("effective_from")
        OffsetDateTime effectiveFrom,

        @JsonProperty("published_at")
        OffsetDateTime publishedAt,

        @JsonProperty("input_snapshot_id")
        String inputSnapshotId,

        @JsonProperty("rule_version")
        String ruleVersion,

        @JsonProperty("statistic_rule_version")
        String statisticRuleVersion,

        /** READY 또는 INPUT_INCOMPLETE. READY가 아니면 평가에 쓰지 않는다. */
        @JsonProperty("quality_status")
        String qualityStatus,

        @JsonProperty("routine_baselines")
        List<RoutineBaseline> routineBaselines,

        List<Statistic> statistics

) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RoutineBaseline(

            @JsonProperty("appliance_type")
            String applianceType,

            /** OVERALL 또는 WEEKDAY. */
            @JsonProperty("baseline_scope")
            String baselineScope,

            /** OVERALL이면 null, WEEKDAY이면 MON~SUN. */
            String weekday,

            @JsonProperty("sample_days")
            Integer sampleDays,

            @JsonProperty("active_days")
            Integer activeDays,

            @JsonProperty("daily_use_probability")
            BigDecimal dailyUseProbability,

            @JsonProperty("reliability_weight")
            BigDecimal reliabilityWeight,

            @JsonProperty("first_use_time_p50_second")
            Integer firstUseTimeP50Second,

            @JsonProperty("expected_until_second")
            Integer expectedUntilSecond,

            @JsonProperty("preferred_window_start_second")
            Integer preferredWindowStartSecond,

            @JsonProperty("preferred_window_end_second")
            Integer preferredWindowEndSecond,

            @JsonProperty("quality_status")
            String qualityStatus,

            Boolean enabled
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Statistic(

            @JsonProperty("metric_name")
            String metricName,

            /** 가구 전체 지표면 null. */
            @JsonProperty("appliance_type")
            String applianceType,

            @JsonProperty("weekday_group")
            String weekdayGroup,

            /** 시간대를 나누지 않는 지표면 null. */
            @JsonProperty("time_bucket")
            String timeBucket,

            @JsonProperty("sample_count")
            Long sampleCount,

            @JsonProperty("eligible_day_count")
            Long eligibleDayCount,

            Double p50,

            Double p90,

            Double mad,

            String unit,

            @JsonProperty("quality_status")
            String qualityStatus
    ) {
    }
}

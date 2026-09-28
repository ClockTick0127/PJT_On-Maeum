package com.nilm.monitoring.service;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 평가 한 번이 입력으로 쓰는 프로필 한 벌.
 *
 * <p>DB에서 한 번에 읽어 값으로 고정한다. 평가 도중 더 새로운 프로필이 도착해도
 * 이미 시작한 평가의 근거는 바뀌지 않는다.
 *
 * @param stale 평가 시각(KST 날짜)과 as_of_date의 간격이 유효기간을 넘었는지.
 *              쓸지 말지는 이 객체를 받은 평가 로직이 정한다.
 */
public record ResolvedProfile(
        String profileVersion,
        LocalDate asOfDate,
        OffsetDateTime effectiveFrom,
        boolean stale,
        List<RoutineBaseline> baselines,
        Map<String, Statistic> statistics
) {

    public record RoutineBaseline(
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
    }

    public record Statistic(
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
    }

    /**
     * 통계 한 줄을 찾는 키. 유니크 제약과 같은 열 조합을 쓰고,
     * 비어 있는 열은 빈 문자열로 맞춰 null 때문에 키가 어긋나지 않게 한다.
     */
    public static String statisticKey(
            String metricName,
            String applianceType,
            String weekdayGroup,
            String timeBucket
    ) {
        return String.join(
                "|",
                metricName == null ? "" : metricName,
                applianceType == null ? "" : applianceType,
                weekdayGroup == null ? "" : weekdayGroup,
                timeBucket == null ? "" : timeBucket
        );
    }

    public Optional<Statistic> statistic(
            String metricName,
            String applianceType,
            String weekdayGroup,
            String timeBucket
    ) {
        return Optional.ofNullable(statistics.get(
                statisticKey(metricName, applianceType, weekdayGroup, timeBucket)));
    }
}

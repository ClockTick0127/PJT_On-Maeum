package com.nilm.monitoring.dto;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;

/**
 * 대상자 상세 화면에서 지금 반영된 생활 프로필을 확인하는 응답.
 * 평가에 쓰는 값 전부가 아니라, 가전별 OVERALL 기준선 요약만 담는다.
 */
public record SubjectProfileResponse(
        String subjectId,
        String profileVersion,
        LocalDate asOfDate,
        LocalDate windowStartDate,
        LocalDate windowEndDate,
        OffsetDateTime effectiveFrom,
        OffsetDateTime publishedAt,
        OffsetDateTime receivedAt,
        String qualityStatus,
        String ruleVersion,
        String statisticRuleVersion,
        List<ApplianceBaseline> appliances
) {

    public record ApplianceBaseline(
            String applianceType,
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
}

package com.nilm.monitoring.dto;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;

/**
 * 대상자 상세 화면의 하루 전력 사용 패턴 그래프 응답.
 * 구간은 {@code timezone} 기준 0시부터 23시까지 항상 24개를 채워서 내려보낸다.
 */
public record SubjectPowerUsageResponse(
        String subjectId,
        LocalDate date,
        String timezone,
        int intervalMinutes,
        String unit,
        BigDecimal totalUsage,
        List<HourlyUsage> hourlyUsage,
        List<ApplianceUsage> appliances,
        OffsetDateTime updatedAt
) {

    /**
     * 구간별 집계 상태.
     *
     * <p>지나간 시간이라고 다 같지 않다. 스냅샷이 끊긴 구간의 0은 전기를 안 썼다는 뜻이 아니라
     * 우리가 보지 못했다는 뜻이므로 {@code NO_DATA}로 갈라 둔다. 값을 믿을 수 없는 구간은
     * {@code usage}도 null로 내려 그래프에 0짜리 막대가 서지 않게 한다.
     */
    public enum BucketStatus {
        COMPLETE, // 구간이 끝났고 관측도 충분히 덮였음
        PARTIAL,  // 구간이 진행 중이거나, 끝났지만 일부 시간만 관측됨
        NO_DATA,  // 지나간 구간인데 관측이 전혀 없음 (usage는 null)
        NOT_YET   // 아직 도달하지 않은 시간 (usage는 null)
    }

    public record HourlyUsage(
            int hour,
            BigDecimal usage,
            BucketStatus status
    ) {
    }

    public record ApplianceUsage(
            String applianceType,
            BigDecimal totalUsage,
            List<HourlyUsage> hourlyUsage
    ) {
    }
}

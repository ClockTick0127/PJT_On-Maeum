package com.nilm.monitoring.service;

import com.nilm.monitoring.config.PowerUsageProperties;
import com.nilm.monitoring.domain.HourlyAppliancePowerUsage;
import com.nilm.monitoring.domain.HourlyPowerUsage;
import com.nilm.monitoring.domain.Subject;
import com.nilm.monitoring.dto.SubjectPowerUsageResponse;
import com.nilm.monitoring.dto.SubjectPowerUsageResponse.BucketStatus;
import com.nilm.monitoring.dto.SubjectPowerUsageResponse.HourlyUsage;
import com.nilm.monitoring.repository.HourlyAppliancePowerUsageRepository;
import com.nilm.monitoring.repository.HourlyPowerUsageRepository;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 대상자 상세 화면의 하루 전력 사용 패턴 그래프를 만든다.
 * 저장은 UTC 정각 구간이고, 응답은 {@link #ZONE} 기준 0~23시 24개 구간으로 고정한다.
 */
@Service
@RequiredArgsConstructor
public class SubjectPowerUsageService {

    private static final ZoneId ZONE = ZoneId.of("Asia/Seoul");
    private static final int BUCKET_COUNT = 24;
    private static final int INTERVAL_MINUTES = 60;
    private static final String UNIT = "Wh";
    private static final int USAGE_SCALE = 1;

    private final SubjectAccessGuard accessGuard;
    private final PowerUsageProperties properties;
    private final HourlyPowerUsageRepository totals;
    private final HourlyAppliancePowerUsageRepository appliances;

    @Transactional(readOnly = true)
    public SubjectPowerUsageResponse getPowerUsage(
            String authSub,
            Long subjectId,
            LocalDate requestedDate
    ) {
        Subject subject = accessGuard.requireReadable(authSub, subjectId);

        LocalDate date = requestedDate == null ? LocalDate.now(ZONE) : requestedDate;
        ZonedDateTime dayStart = date.atStartOfDay(ZONE);
        OffsetDateTime from = dayStart.toOffsetDateTime();
        OffsetDateTime until = dayStart.plusDays(1).toOffsetDateTime();
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

        List<HourlyPowerUsage> totalRows = totals
                .findAllByHouseholdIdAndBucketStartAtGreaterThanEqualAndBucketStartAtLessThanOrderByBucketStartAtAsc(
                        subject.getHouseholdId(), from, until);
        List<HourlyAppliancePowerUsage> applianceRows = appliances
                .findAllByHouseholdIdAndBucketStartAtGreaterThanEqualAndBucketStartAtLessThanOrderByApplianceTypeAscBucketStartAtAsc(
                        subject.getHouseholdId(), from, until);

        BigDecimal[] totalBuckets = emptyBuckets();
        // 관측 커버리지는 가구 단위다. 가전별 계열도 같은 값으로 상태를 정한다.
        int[] observedSeconds = new int[BUCKET_COUNT];
        for (HourlyPowerUsage row : totalRows) {
            add(totalBuckets, dayStart, row.getBucketStartAt(), row.getEnergyWh());
            int hour = hourIndex(dayStart, row.getBucketStartAt());
            if (hour >= 0) {
                observedSeconds[hour] += row.getObservedSeconds();
            }
        }

        // 가전 코드 순으로 고정해 프론트가 매번 같은 순서의 계열을 그린다.
        Map<String, BigDecimal[]> byAppliance = new TreeMap<>();
        // 그날 한 번도 쓰지 않은 가전도 계열을 세운다. 배열에서 빠지면 화면이 가전 행을
        // 직접 채워 넣어야 하고, 안 쓴 것과 목록에 없는 것이 같아 보인다.
        for (String applianceType : properties.applianceTypes()) {
            byAppliance.put(applianceType, emptyBuckets());
        }
        for (HourlyAppliancePowerUsage row : applianceRows) {
            add(
                    byAppliance.computeIfAbsent(row.getApplianceType(), key -> emptyBuckets()),
                    dayStart,
                    row.getBucketStartAt(),
                    row.getEnergyWh()
            );
        }

        List<HourlyUsage> hourlyUsage =
                toHourlyUsage(totalBuckets, observedSeconds, dayStart, now);
        List<SubjectPowerUsageResponse.ApplianceUsage> applianceUsages = byAppliance.entrySet().stream()
                .map(entry -> {
                    List<HourlyUsage> buckets =
                            toHourlyUsage(entry.getValue(), observedSeconds, dayStart, now);
                    return new SubjectPowerUsageResponse.ApplianceUsage(
                            entry.getKey(), sum(buckets), buckets);
                })
                .toList();

        return new SubjectPowerUsageResponse(
                subject.getId().toString(),
                date,
                ZONE.getId(),
                INTERVAL_MINUTES,
                UNIT,
                sum(hourlyUsage),
                hourlyUsage,
                applianceUsages,
                latestUpdatedAt(totalRows, applianceRows)
        );
    }

    private BigDecimal[] emptyBuckets() {
        BigDecimal[] buckets = new BigDecimal[BUCKET_COUNT];
        java.util.Arrays.fill(buckets, BigDecimal.ZERO);
        return buckets;
    }

    /**
     * 저장 구간 시작 시각을 화면 기준 시(0~23)로 옮겨 담는다.
     * 조회 범위 밖의 값은 들어올 수 없지만, 방어적으로 무시한다.
     */
    private void add(
            BigDecimal[] buckets,
            ZonedDateTime dayStart,
            OffsetDateTime bucketStartAt,
            BigDecimal energyWh
    ) {
        int hour = hourIndex(dayStart, bucketStartAt);
        if (hour < 0) {
            return;
        }
        buckets[hour] = buckets[hour].add(energyWh == null ? BigDecimal.ZERO : energyWh);
    }

    /** 조회 범위 밖의 값은 들어올 수 없지만, 방어적으로 -1을 돌려준다. */
    private int hourIndex(ZonedDateTime dayStart, OffsetDateTime bucketStartAt) {
        int hour = (int) java.time.Duration
                .between(dayStart.toInstant(), bucketStartAt.toInstant())
                .toHours();
        return hour < 0 || hour >= BUCKET_COUNT ? -1 : hour;
    }

    private List<HourlyUsage> toHourlyUsage(
            BigDecimal[] buckets,
            int[] observedSeconds,
            ZonedDateTime dayStart,
            OffsetDateTime now
    ) {
        List<HourlyUsage> result = new ArrayList<>(BUCKET_COUNT);
        for (int hour = 0; hour < BUCKET_COUNT; hour++) {
            OffsetDateTime bucketStart = dayStart.plusHours(hour).toOffsetDateTime();
            OffsetDateTime bucketEnd = dayStart.plusHours(hour + 1L).toOffsetDateTime();
            BucketStatus status =
                    statusOf(bucketStart, bucketEnd, now, observedSeconds[hour]);
            // 믿을 수 없는 구간은 0이 아니라 null이다. 0짜리 막대는 "안 썼다"로 읽힌다.
            BigDecimal usage =
                    status == BucketStatus.NOT_YET || status == BucketStatus.NO_DATA
                            ? null
                            : buckets[hour].setScale(USAGE_SCALE, RoundingMode.HALF_UP);
            result.add(new HourlyUsage(hour, usage, status));
        }
        return result;
    }

    /**
     * 시각과 관측 커버리지를 함께 본다.
     * 시각만으로 정하면 스냅샷이 끊긴 구간도 0으로 확정돼 "그 시간에 조용했다"로 보인다.
     */
    private BucketStatus statusOf(
            OffsetDateTime bucketStart,
            OffsetDateTime bucketEnd,
            OffsetDateTime now,
            int observedSeconds
    ) {
        if (!now.isAfter(bucketStart)) {
            return BucketStatus.NOT_YET;
        }
        if (observedSeconds <= 0) {
            return BucketStatus.NO_DATA;
        }
        if (now.isBefore(bucketEnd)) {
            // 아직 차는 중이다. 커버리지가 모자란 것이 당연하므로 따지지 않는다.
            return BucketStatus.PARTIAL;
        }
        return observedSeconds < requiredSeconds()
                ? BucketStatus.PARTIAL
                : BucketStatus.COMPLETE;
    }

    private int requiredSeconds() {
        return (int) Math.ceil(
                INTERVAL_MINUTES * 60 * properties.getCompleteCoverageRatio());
    }

    private BigDecimal sum(List<HourlyUsage> buckets) {
        BigDecimal total = BigDecimal.ZERO;
        for (HourlyUsage bucket : buckets) {
            if (bucket.usage() != null) {
                total = total.add(bucket.usage());
            }
        }
        return total.setScale(USAGE_SCALE, RoundingMode.HALF_UP);
    }

    /** 그래프가 어느 시점까지의 집계인지 알려주는 값. 데이터가 없으면 null. */
    private OffsetDateTime latestUpdatedAt(
            List<HourlyPowerUsage> totalRows,
            List<HourlyAppliancePowerUsage> applianceRows
    ) {
        OffsetDateTime latest = null;
        for (HourlyPowerUsage row : totalRows) {
            latest = later(latest, row.getUpdatedAt());
        }
        for (HourlyAppliancePowerUsage row : applianceRows) {
            latest = later(latest, row.getUpdatedAt());
        }
        return latest;
    }

    private OffsetDateTime later(OffsetDateTime current, OffsetDateTime candidate) {
        if (candidate == null) {
            return current;
        }
        return current == null || candidate.isAfter(current) ? candidate : current;
    }
}

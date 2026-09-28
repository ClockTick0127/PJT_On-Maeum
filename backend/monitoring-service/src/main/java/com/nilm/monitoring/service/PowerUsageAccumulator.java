package com.nilm.monitoring.service;

import com.nilm.monitoring.config.PowerUsageProperties;
import com.nilm.monitoring.domain.HourlyAppliancePowerUsage;
import com.nilm.monitoring.domain.HourlyAppliancePowerUsageId;
import com.nilm.monitoring.domain.HourlyPowerUsage;
import com.nilm.monitoring.domain.HourlyPowerUsageId;
import com.nilm.monitoring.dto.kafka.AnalysisSnapshotMessage;
import com.nilm.monitoring.repository.HourlyAppliancePowerUsageRepository;
import com.nilm.monitoring.repository.HourlyPowerUsageRepository;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * 스냅샷의 순시 전력을 시간 구간 전력량으로 적산한다.
 *
 * <p>합산 전력량은 실측이다. 두 스냅샷 사이를 사다리꼴로 적분해 구간에 나눠 담는다.
 * 가전별 전력량은 추정이다. 가전별 소비 전력은 어디에도 없으므로, 기저부하를 뺀 실측
 * 전력량을 그 시점 ON인 가전들에게 가중치 비율로 나눈다. 배분이므로 가전별 합은 항상
 * 합산 전력량 이하이고, 차이가 6종 밖의 상시 부하다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PowerUsageAccumulator {

    /** 나눗셈 중간값의 자릿수. 저장 자릿수보다 넉넉히 둬 반올림을 마지막에 한 번만 한다. */
    private static final int WORKING_SCALE = 9;

    /** {@code energy_wh} 칼럼의 자릿수. */
    private static final int STORAGE_SCALE = 6;

    private static final BigDecimal SECONDS_PER_HOUR = new BigDecimal("3600");
    private static final BigDecimal TWO = new BigDecimal("2");

    private final PowerUsageProperties properties;
    private final LatestSnapshotStore snapshots;
    private final HourlyPowerUsageRepository totals;
    private final HourlyAppliancePowerUsageRepository appliances;

    /**
     * 직전 스냅샷과의 사이를 적산해 반영한다.
     * 전환이 없어도 전력은 계속 쓰이므로 모든 스냅샷에서 불러야 한다.
     */
    public void accumulate(AnalysisSnapshotMessage message, OffsetDateTime now) {
        BigDecimal power = activePower(message);
        if (power == null) {
            // 계약상 있어야 하는 값이지만, 없거나 음수면 적분할 수 없다.
            // 기준점도 옮기지 않는다. 다음 스냅샷이 공백 판정으로 걸러진다.
            return;
        }

        OffsetDateTime observedAt = message.observedAt();
        LatestSnapshot previous = snapshots.load(message.householdId()).orElse(null);
        if (previous != null && !observedAt.isAfter(previous.observedAt())) {
            // 재전송이거나 뒤늦게 도착한 과거 스냅샷이다. 기준점을 되돌리지 않는다.
            return;
        }

        // 기준점을 먼저 옮긴다. 아래에서 롤백되면 이 구간의 전력량을 잃지만,
        // 재전송이 같은 구간을 다시 더하는 일은 없다.
        snapshots.save(message.householdId(), new LatestSnapshot(observedAt, power));
        if (previous == null) {
            return;
        }

        long seconds = Duration.between(previous.observedAt(), observedAt).getSeconds();
        if (seconds > properties.getGapThreshold().getSeconds()) {
            log.debug("스냅샷이 끊긴 구간은 적산하지 않는다: householdId={}, 간격={}초",
                    message.householdId(), seconds);
            return;
        }

        // 사다리꼴. 간격이 짧아 직사각형과 차이는 작지만, ON/OFF 계단의 오차를
        // 앞뒤 구간에 대칭으로 나눠 갖는다.
        BigDecimal averageWatts = previous.activePower().add(power).divide(TWO);
        BigDecimal energyWh = averageWatts
                .multiply(BigDecimal.valueOf(seconds))
                .divide(SECONDS_PER_HOUR, WORKING_SCALE, RoundingMode.HALF_UP);

        List<String> poweredOn = poweredOn(message);
        OffsetDateTime cursor = previous.observedAt();
        while (cursor.isBefore(observedAt)) {
            OffsetDateTime bucketStart = cursor
                    .withOffsetSameInstant(ZoneOffset.UTC)
                    .truncatedTo(ChronoUnit.HOURS);
            OffsetDateTime sliceEnd = earlier(bucketStart.plusHours(1), observedAt);
            long sliceSeconds = Duration.between(cursor, sliceEnd).getSeconds();

            // 구간 경계를 넘는 간격은 시간 비례로 쪼갠다.
            BigDecimal sliceWh = energyWh
                    .multiply(BigDecimal.valueOf(sliceSeconds))
                    .divide(BigDecimal.valueOf(seconds), WORKING_SCALE, RoundingMode.HALF_UP);

            addTotal(message.householdId(), bucketStart, sliceWh, sliceSeconds, now);
            allocate(message.householdId(), bucketStart, sliceWh, sliceSeconds, poweredOn, now);

            cursor = sliceEnd;
        }
    }

    /**
     * 기저부하를 뺀 나머지를 ON인 가전들에게 가중치 비율로 나눈다.
     *
     * <p>기저부하를 먼저 빼지 않으면 전기포트만 켜진 3분 동안 냉장고와 대기전력까지
     * 전기포트 몫으로 붙는다. 뺀 나머지는 어느 가전에도 속하지 않으므로 기록하지 않는다.
     */
    private void allocate(
            String householdId,
            OffsetDateTime bucketStart,
            BigDecimal sliceWh,
            long sliceSeconds,
            List<String> poweredOn,
            OffsetDateTime now
    ) {
        if (poweredOn.isEmpty()) {
            return;
        }

        BigDecimal baseWh = properties.getBaseLoadWatts()
                .multiply(BigDecimal.valueOf(sliceSeconds))
                .divide(SECONDS_PER_HOUR, WORKING_SCALE, RoundingMode.HALF_UP);
        BigDecimal allocatable = sliceWh.subtract(baseWh);
        if (allocatable.signum() <= 0) {
            // 측정값이 기저부하에 못 미친다. 없는 전력량을 만들어 내지 않는다.
            return;
        }

        BigDecimal weightSum = BigDecimal.ZERO;
        for (String applianceType : poweredOn) {
            weightSum = weightSum.add(properties.weightOf(applianceType));
        }
        if (weightSum.signum() <= 0) {
            return;
        }

        for (String applianceType : poweredOn) {
            BigDecimal share = allocatable
                    .multiply(properties.weightOf(applianceType))
                    .divide(weightSum, WORKING_SCALE, RoundingMode.HALF_UP);
            addAppliance(householdId, bucketStart, applianceType, share, now);
        }
    }

    /**
     * 이 스냅샷 시점에 켜져 있고 가중치를 아는 가전들.
     * 간격이 짧아 구간 내내 같은 상태였다고 본다.
     */
    private List<String> poweredOn(AnalysisSnapshotMessage message) {
        List<String> result = new ArrayList<>();
        for (AnalysisSnapshotMessage.Appliance appliance : message.appliances()) {
            if (appliance == null || !Boolean.TRUE.equals(appliance.isOn())) {
                continue;
            }
            BigDecimal weight = properties.weightOf(appliance.applianceType());
            if (weight == null || weight.signum() <= 0) {
                // 가중치를 모르는 가전은 배분에서 빠진다. 그 몫은 기록되지 않는다.
                log.debug("배분 가중치가 없는 가전은 건너뛴다: applianceType={}",
                        appliance.applianceType());
                continue;
            }
            result.add(appliance.applianceType());
        }
        return result;
    }

    /**
     * 같은 가구의 스냅샷은 한 파티션으로 순서대로 들어오므로 읽고 더하는 사이에
     * 끼어들 경쟁자가 없다. {@code recordObservation}과 같은 전제다.
     */
    private void addTotal(
            String householdId,
            OffsetDateTime bucketStart,
            BigDecimal amount,
            long seconds,
            OffsetDateTime now
    ) {
        // 전력량이 0으로 반올림되어도 행은 남긴다. 관측한 시간이 곧 "봤다"는 증거이고,
        // 그게 없으면 조용한 시간과 끊긴 시간을 구분할 수 없다.
        BigDecimal stored = amount.max(BigDecimal.ZERO)
                .setScale(STORAGE_SCALE, RoundingMode.HALF_UP);
        int observed = Math.toIntExact(seconds);
        totals.findById(new HourlyPowerUsageId(householdId, bucketStart))
                .ifPresentOrElse(
                        row -> row.add(stored, observed, now),
                        () -> totals.save(new HourlyPowerUsage(
                                householdId, bucketStart, stored, observed, now))
                );
    }

    private void addAppliance(
            String householdId,
            OffsetDateTime bucketStart,
            String applianceType,
            BigDecimal amount,
            OffsetDateTime now
    ) {
        BigDecimal stored = amount.setScale(STORAGE_SCALE, RoundingMode.HALF_UP);
        if (stored.signum() <= 0) {
            return;
        }
        appliances
                .findById(new HourlyAppliancePowerUsageId(
                        householdId, bucketStart, applianceType))
                .ifPresentOrElse(
                        row -> row.add(stored, now),
                        () -> appliances.save(new HourlyAppliancePowerUsage(
                                householdId, bucketStart, applianceType, stored, now))
                );
    }

    /** 적분에 쓸 수 있는 유효 전력만 돌려준다. */
    private BigDecimal activePower(AnalysisSnapshotMessage message) {
        if (message.measurement() == null) {
            return null;
        }
        BigDecimal power = message.measurement().activePower();
        if (power == null || power.signum() < 0) {
            return null;
        }
        return power;
    }

    private OffsetDateTime earlier(OffsetDateTime left, OffsetDateTime right) {
        return left.isBefore(right) ? left : right;
    }
}

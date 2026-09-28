package com.nilm.monitoring.risk;

import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.OptionalLong;

/**
 * 평가가 현재 값을 믿어도 되는 근거.
 *
 * <p>두 가지를 따로 싣는다. <b>신선도</b>는 마지막 관측이 얼마나 최근인가이고,
 * <b>커버리지</b>는 비교하려는 구간을 실제로 얼마나 봤는가다. 스냅샷 한 건이 방금 도착했다는
 * 사실은 신선도만 회복시킨다. 그 앞의 공백은 여전히 공백이라, "오늘 한 번도 쓰지 않았다"
 * 같은 하루치 단정은 커버리지가 받쳐 줘야 한다.
 *
 * @param lastObservedAt 마지막으로 도착한 스냅샷의 관측 시각
 * @param coverageTracked 커버리지를 집계하는 입력인가. 아니면 "모른다"는 뜻이고
 *                        비율을 0으로 보지 않고 지표를 제외한다
 * @param continuousSince 끊김 없이 이어진 관측의 시작. 모르면 null
 * @param coveredSecondsByDate 영업일별로 실제 관측한 초
 */
public record ObservationQuality(
        OffsetDateTime lastObservedAt,
        boolean coverageTracked,
        OffsetDateTime continuousSince,
        Map<LocalDate, Long> coveredSecondsByDate
) {

    public ObservationQuality {
        coveredSecondsByDate = coveredSecondsByDate == null
                ? Map.of()
                : Map.copyOf(coveredSecondsByDate);
    }

    /** 커버리지를 알 수 없는 입력. 마지막 관측 시각만 들고 있다. */
    public static ObservationQuality untracked(OffsetDateTime lastObservedAt) {
        return new ObservationQuality(lastObservedAt, false, null, Map.of());
    }

    /**
     * {@code [windowStart, until]} 구간 중 실제로 관측한 비율.
     *
     * <p>꼬리(마지막 관측 이후 지금까지)는 세지 않는다. 그 구간은 신선도 판정이 맡는다.
     * 두 규칙이 같은 공백을 두 번 깎으면 정상적으로 흐르는 가구도 비율이 떨어진다.
     *
     * @return 집계하지 않은 날이면 비어 있다
     */
    public OptionalLong coveredSeconds(LocalDate date) {
        Long seconds = coveredSecondsByDate.get(date);
        return seconds == null ? OptionalLong.empty() : OptionalLong.of(seconds);
    }

    /** 구간 대비 관측 비율. 아직 볼 시간이 없었으면 1.0이다. */
    public double coverageRatio(LocalDate date, OffsetDateTime windowStart, OffsetDateTime until) {
        OptionalLong covered = coveredSeconds(date);
        if (covered.isEmpty()) {
            return 0;
        }
        OffsetDateTime windowEnd = lastObservedAt != null && lastObservedAt.isBefore(until)
                ? lastObservedAt
                : until;
        long window = Duration.between(windowStart, windowEnd).getSeconds();
        if (window <= 0) {
            return 1.0;
        }
        return Math.min(1.0, (double) covered.getAsLong() / window);
    }

    /** 주어진 시각부터 지금까지 관측이 한 번도 끊기지 않았는가. */
    public boolean continuousFrom(OffsetDateTime from) {
        return continuousSince != null && !continuousSince.isAfter(from);
    }
}

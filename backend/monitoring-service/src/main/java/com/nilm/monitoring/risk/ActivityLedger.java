package com.nilm.monitoring.risk;

import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalInt;

/**
 * 평가가 보는 "사용 사실" 한 벌.
 *
 * <p>Gold가 한 번의 사용으로 세는 단위와 같은 뜻을 가져야 한다. 스냅샷의 OFF→ON 전환마다
 * 한 번으로 세면 3초짜리 잡음도, 30초 뒤의 재시작도 각각 한 번이 되어 같은 이름의 숫자가
 * 서로 다른 것을 센다. 그래서 여기에는 병합과 최소 사용시간을 통과한 유효 사용만 담는다.
 *
 * <p>{@code precise}가 거짓인 원장은 전환 횟수만 알던 옛 입력이다. 그 입력으로는 어떤 시작이
 * 유효한지, 기준 시각 이전에 일어났는지 알 수 없어서 루틴 미사용(M)과 활동 감소(A)를
 * 계산하지 않는다. 계산하지 않는 것과 0점은 다르다.
 */
public record ActivityLedger(
        boolean precise,
        List<Use> uses,
        OffsetDateTime lastActivityEndedAt,
        boolean anyApplianceOn,
        Map<String, CurrentState.DailyUsage> todayUsage
) {

    public ActivityLedger {
        uses = uses == null ? List.of() : List.copyOf(uses);
        todayUsage = todayUsage == null ? Map.of() : Map.copyOf(todayUsage);
    }

    /**
     * Gold와 같은 정의로 재구성한 유효 사용들.
     *
     * @param uses 평가 구간에 걸친 유효 사용. 무활동 기준점이 될 마지막 사용을 포함한다
     */
    public static ActivityLedger of(List<Use> uses) {
        return new ActivityLedger(true, uses, null, false, Map.of());
    }

    /**
     * 전환 횟수만 있던 옛 입력. 마지막 활동 시각과 현재 ON 여부로 무활동만 계산한다.
     *
     * @deprecated 평가 경로는 {@code CurrentStateProvider}가 만든 원장으로 갈아탔다
     */
    @Deprecated(since = "monitoring-score-v1-MIA")
    public static ActivityLedger coarse(
            OffsetDateTime lastActivityEndedAt,
            boolean anyApplianceOn,
            Map<String, CurrentState.DailyUsage> todayUsage
    ) {
        return new ActivityLedger(false, List.of(), lastActivityEndedAt, anyApplianceOn, todayUsage);
    }

    /**
     * 유효 사용 한 건.
     *
     * @param startImputed 시작을 보지 못한 사용(첫 스냅샷이 이미 ON). 시작 횟수로 세지 않는다
     * @param businessDate 시작 시각이 속한 영업일. 자정을 넘긴 사용은 시작한 날에만 든다
     */
    public record Use(
            String applianceType,
            OffsetDateTime startedAt,
            OffsetDateTime endedAt,
            boolean startImputed,
            LocalDate businessDate
    ) {

        public boolean activeAt(OffsetDateTime at) {
            return !startedAt.isAfter(at) && (endedAt == null || endedAt.isAfter(at));
        }
    }

    /**
     * 기준 시각에서 잰 무활동 경과 초. 그 시각에 쓰고 있었으면 0이다.
     *
     * @return 비교할 기준점이 없으면 비어 있다. 경과를 0으로도 무한으로도 볼 수 없다
     */
    public OptionalDouble inactivitySeconds(OffsetDateTime at) {
        if (!precise) {
            if (anyApplianceOn) {
                return OptionalDouble.of(0);
            }
            if (lastActivityEndedAt == null) {
                return OptionalDouble.empty();
            }
            if (lastActivityEndedAt.isAfter(at)) {
                // 기준 시각에는 아직 쓰고 있었다.
                return OptionalDouble.of(0);
            }
            return OptionalDouble.of(Duration.between(lastActivityEndedAt, at).getSeconds());
        }

        OffsetDateTime lastEnd = null;
        for (Use use : uses) {
            if (use.activeAt(at)) {
                return OptionalDouble.of(0);
            }
            OffsetDateTime end = use.endedAt();
            if (end != null && !end.isAfter(at) && (lastEnd == null || end.isAfter(lastEnd))) {
                lastEnd = end;
            }
        }
        if (lastEnd == null) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(Duration.between(lastEnd, at).getSeconds());
    }

    /**
     * 영업일이 시작한 뒤 기준 시각 <b>전</b>까지 확정된 유효 사용 시작 횟수.
     *
     * <p>자정을 넘겨 이어진 사용은 시작한 날에만 든다. 새 날의 시작으로 다시 세지 않는다.
     *
     * @return 옛 입력이면 비어 있다
     */
    public OptionalInt startCount(LocalDate businessDate, OffsetDateTime until) {
        if (!precise) {
            return OptionalInt.empty();
        }
        int count = 0;
        for (Use use : uses) {
            if (use.startImputed() || !businessDate.equals(use.businessDate())) {
                continue;
            }
            if (use.startedAt().isBefore(until)) {
                count++;
            }
        }
        return OptionalInt.of(count);
    }

    /**
     * 그 영업일에 이 가전을 썼는가.
     *
     * <p>자정을 넘겨 이어진 사용도 그 날의 사용이다. 새 시작으로 세지는 않지만
     * "오늘 한 번도 쓰지 않았다"를 뒤집기에는 충분하다.
     */
    public boolean used(String applianceType, OffsetDateTime dayStart, OffsetDateTime until) {
        if (!precise) {
            CurrentState.DailyUsage usage = todayUsage.get(applianceType);
            return usage != null && usage.firstOnAt() != null;
        }
        for (Use use : uses) {
            if (!applianceType.equals(use.applianceType())) {
                continue;
            }
            if (use.startedAt().isAfter(until)) {
                continue;
            }
            if (use.endedAt() == null || use.endedAt().isAfter(dayStart)) {
                return true;
            }
        }
        return false;
    }
}

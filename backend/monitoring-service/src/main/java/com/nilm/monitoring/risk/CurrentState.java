package com.nilm.monitoring.risk;

import java.time.OffsetDateTime;
import java.util.Map;

/**
 * 평가 한 번이 보는 "지금"의 값 한 벌.
 *
 * <p>프로필과 마찬가지로 평가 시작 시점에 읽어 값으로 고정한다.
 * 평가 도중 새 스냅샷이 들어와도 이미 시작한 평가의 근거는 바뀌지 않는다.
 *
 * @param now 평가를 시작한 시각. 비교 기준 시각은 여기서 {@link EvaluationPoint}가 뽑는다
 * @param observation 관측 신선도와 커버리지. "보지 못한 것"을 "없었던 것"으로 바꾸지 않는 근거다
 * @param activity Gold와 같은 정의로 센 사용 사실
 */
public record CurrentState(
        OffsetDateTime now,
        boolean away,
        ObservationQuality observation,
        ActivityLedger activity
) {

    /**
     * 옛 입력 계약으로 만드는 상태.
     *
     * <p>평가 경로는 {@code CurrentStateProvider}로 옮겨서 더 이상 이 생성자를 부르지 않는다.
     * 이 입력으로는 관측 커버리지도, 유효 사용 정의도 확인할 수 없어서 루틴 미사용(M)과
     * 활동 감소(A)가 제외된다는 사실을 남겨 두려고 계약만 지킨다. 커버리지를 모르는
     * 입력이 다시 들어오더라도 그 구간을 정상 관측으로 바꾸지 않는다는 뜻이다.
     *
     * @deprecated {@code CurrentStateProvider}가 만든 상태를 쓴다
     */
    @Deprecated(since = "monitoring-score-v1-MIA")
    public CurrentState(
            OffsetDateTime now,
            boolean away,
            OffsetDateTime lastObservedAt,
            OffsetDateTime lastActivityEndedAt,
            boolean anyApplianceOn,
            Map<String, DailyUsage> todayUsage
    ) {
        this(
                now,
                away,
                ObservationQuality.untracked(lastObservedAt),
                ActivityLedger.coarse(lastActivityEndedAt, anyApplianceOn, todayUsage));
    }

    /** 평가 이력에 남기는 현재 입력의 기준 시각. */
    public OffsetDateTime lastObservedAt() {
        return observation.lastObservedAt();
    }

    /**
     * 옛 입력의 당일 사용 사실.
     *
     * @param firstOnAt 오늘 처음 켜진 시각. 아직 안 켰으면 null
     * @param startCount 오늘 OFF→ON 전환 횟수
     * @deprecated 유효 사용 정의를 통과하지 않은 숫자다. {@link ActivityLedger.Use}를 쓴다
     */
    @Deprecated(since = "monitoring-score-v1-MIA")
    public record DailyUsage(OffsetDateTime firstOnAt, int startCount) {
    }
}

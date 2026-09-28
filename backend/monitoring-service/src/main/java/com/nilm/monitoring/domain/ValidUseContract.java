package com.nilm.monitoring.domain;

import java.time.Duration;
import java.util.Map;

/**
 * Gold가 "한 번의 사용"으로 세는 기준. 모니터링의 실시간 입력도 같은 뜻을 갖게 하려고
 * 배치의 규칙을 그대로 옮겨 둔다.
 *
 * <p>원본은 {@code batch/power_silver_service/src/power_silver/appliance_usage.py}와
 * {@code batch/gold_profile_service/src/gold_profile/logical_uses.py}다. 두 곳 모두
 *
 * <ul>
 *   <li>같은 가전의 ON 구간 사이 간격이 병합 간격 <b>이하</b>면 한 번의 사용으로 묶고,</li>
 *   <li>묶은 뒤 실제 사용시간 합계가 10초 미만이면 TOO_SHORT로 버린다.</li>
 * </ul>
 *
 * <p>정확히 10초는 유효하다. 배치의 조건이 {@code active_duration_us < 10_000_000}이기 때문이다.
 *
 * <p>여기 값들은 위험 점수 정책이 아니라 배치와 맞춰야 하는 계약이다. 그래서
 * {@code application.properties}로 빼지 않는다. 배치가 바꾸면 여기도 같이 바꾼다.
 */
public final class ValidUseContract {

    /** 실제 사용시간 합계가 이보다 짧으면 사용으로 세지 않는다. */
    public static final Duration MINIMUM_ACTIVE = Duration.ofSeconds(10);

    /** 배치의 MERGE_GAP_SECONDS. 표에 없는 가전은 기본값을 쓴다. */
    private static final Map<String, Duration> MERGE_GAPS = Map.of(
            "KETTLE", Duration.ofSeconds(60),
            "MICROWAVE", Duration.ofSeconds(60),
            "HAIR_DRYER", Duration.ofSeconds(60),
            "VACUUM_CLEANER", Duration.ofSeconds(60),
            "INDUCTION", Duration.ofSeconds(120),
            "IRON", Duration.ofSeconds(300)
    );

    private static final Duration DEFAULT_MERGE_GAP = Duration.ofSeconds(60);

    private ValidUseContract() {
    }

    /**
     * 직전 사용이 끝난 뒤 이 간격 <b>이내</b>에 다시 켜지면 같은 사용으로 본다.
     * 배치가 {@code (start - previous_end) > merge_gap}일 때만 새 사용으로 세므로 경계는 병합 쪽이다.
     */
    public static Duration mergeGap(String applianceType) {
        return MERGE_GAPS.getOrDefault(applianceType, DEFAULT_MERGE_GAP);
    }

    /** 실제 사용시간 합계가 유효 기준을 채웠는가. 정확히 기준값이면 유효다. */
    public static boolean valid(long activeSeconds) {
        return activeSeconds >= MINIMUM_ACTIVE.getSeconds();
    }
}

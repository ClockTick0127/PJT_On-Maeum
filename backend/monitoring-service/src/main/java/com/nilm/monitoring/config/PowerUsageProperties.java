package com.nilm.monitoring.config;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 시간별 전력 사용량 적산이 쓰는 설정.
 *
 * <p>합산 전력량은 스냅샷의 {@code active_power}를 적분한 실측값이지만, 가전별 전력량은
 * 실측이 아니다. NILM 분석은 가전별 ON/OFF만 내놓고 가전별 소비 전력은 어디에도 없어서,
 * 기저부하를 뺀 실측 전력량을 그 시점 ON인 가전들에게 가중치 비율로 나눈 추정치다.
 */
@Getter
@Setter
@ConfigurationProperties("app.power-usage")
public class PowerUsageProperties {

    /**
     * 이 간격보다 길게 벌어진 두 스냅샷 사이는 적분하지 않는다.
     * 30분 끊긴 뒤 들어온 값을 직전 값과 이어 붙이면 없던 전력량이 생긴다.
     * 분석 서비스의 {@code analysis_data_gap_threshold_seconds}와 같은 값을 기본으로 둔다.
     */
    private Duration gapThreshold = Duration.ofSeconds(120);

    /**
     * 6종 가전에 속하지 않는 상시 부하(대기전력 + 냉장고)의 시간 평균 전력(W).
     * 시뮬레이터 기준으로 대기전력 40~65W에 냉장고 55~85W가 가동 20분/정지 27.5분 주기로
     * 더해져 약 82W다. 냉장고 주기 때문에 순시값은 52W와 122W 사이를 오가므로
     * 순시로 빼지 않고 구간 단위로만 뺀다.
     */
    private BigDecimal baseLoadWatts = new BigDecimal("82");

    /**
     * 가전별 배분 가중치(W). AI Hub 71685 데이터셋 실측 중앙값이며
     * {@code infrastructure/mqtt/simulator/engine/profiles.py}와 같은 출처다.
     *
     * <p>인덕션과 다리미는 듀티 사이클 가전이라 실측 중앙값이 가열 구간의 전력이다.
     * 세션 병합이 듀티 휴지를 덮어 {@code is_on}은 세션 내내 true이므로, 정격을 그대로 쓰면
     * 이 둘만 과대 배분된다. 그래서 듀티비를 곱한 세션 평균 전력을 가중치로 쓴다.
     * (인덕션 1463W × 18/28 ≈ 940W, 다리미 1389W × 18/38 ≈ 658W)
     */
    private Map<String, BigDecimal> applianceWeights = new LinkedHashMap<>(Map.of(
            "KETTLE", new BigDecimal("1657"),
            "INDUCTION", new BigDecimal("940"),
            "IRON", new BigDecimal("658"),
            "MICROWAVE", new BigDecimal("941"),
            "HAIR_DRYER", new BigDecimal("934"),
            "VACUUM_CLEANER", new BigDecimal("819")
    ));

    /**
     * 끝난 구간을 COMPLETE로 볼 관측 커버리지 하한(0~1).
     *
     * <p>한 시간을 온전히 봤어도 공백 판정에 걸린 간격이나 구간 첫 기준점 때문에
     * 3600초를 꽉 채우지는 않는다. 이 값에 못 미치면 값이 낮게 나온 것이 실제인지
     * 덜 본 탓인지 알 수 없으므로 PARTIAL로 알린다.
     */
    private double completeCoverageRatio = 0.9;

    /**
     * Redis에 둔 최신 스냅샷의 보관 기간.
     * 만료되면 다음 스냅샷은 적분 기준점을 잃어 그 구간 하나만 건너뛴다.
     */
    private Duration snapshotTtl = Duration.ofHours(1);

    /**
     * 가중치를 찾는다. 설정 키는 완화된 바인딩 때문에 {@code hair-dryer}로 들어올 수 있어
     * 가전 코드 형태로 맞춰 본다. 표에 없는 가전은 배분에서 제외한다.
     */
    public BigDecimal weightOf(String applianceType) {
        if (applianceType == null) {
            return null;
        }
        String code = normalize(applianceType);
        for (Map.Entry<String, BigDecimal> entry : applianceWeights.entrySet()) {
            if (normalize(entry.getKey()).equals(code)) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * 그래프가 계열을 세울 가전 목록.
     *
     * <p>가중치 표를 그대로 쓴다. 가중치가 없는 가전은 배분을 받지 못해 값이 늘 비므로
     * 두 목록이 갈라질 이유가 없다.
     */
    public Set<String> applianceTypes() {
        Set<String> types = new LinkedHashSet<>();
        for (String key : applianceWeights.keySet()) {
            types.add(normalize(key));
        }
        return types;
    }

    private String normalize(String applianceType) {
        return applianceType.toUpperCase(Locale.ROOT).replace('-', '_');
    }
}

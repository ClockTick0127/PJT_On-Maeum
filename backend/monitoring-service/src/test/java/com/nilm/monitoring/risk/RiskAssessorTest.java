package com.nilm.monitoring.risk;

import static org.assertj.core.api.Assertions.assertThat;

import com.nilm.monitoring.service.ResolvedProfile;
import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * 점수식 자체를 Spring 없이 검증한다.
 *
 * <p>기준 시각은 KST 2026-09-21(월) 12:10으로 고정한다. 시간대 구간이 결과를 좌우하므로
 * "지금"이 흔들리면 단정도 흔들린다.
 *
 * <p>프로필 통계와 맞대는 시각은 12:10이 아니라 그 이하의 가장 최근 구간 경계인 12:00이다.
 * 프로필의 "12:30"은 12:30 정각에 잰 값이라, 12:10에 그 줄을 꺼내 쓰면 아직 오지 않은
 * 20분치 활동을 이미 했어야 하는 것으로 비교하게 된다.
 */
class RiskAssessorTest {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    private static final OffsetDateTime NOW = OffsetDateTime.parse("2026-09-21T12:10:00+09:00");

    /** 비교 기준 시각. 12:10 이하의 가장 최근 구간 경계다. */
    private static final OffsetDateTime EVAL = OffsetDateTime.parse("2026-09-21T12:00:00+09:00");

    /** 배치 표기를 따라 구간의 끝 시각만 적는다. 기준 시각 12:00의 구간 이름이다. */
    private static final String BUCKET = "12:00";

    private static final LocalDate TODAY = LocalDate.of(2026, 9, 21);

    private static final OffsetDateTime DAY_START =
            OffsetDateTime.parse("2026-09-21T00:00:00+09:00");

    private static final String ACTIVITY = "CUMULATIVE_ACTIVITY_START_COUNT";

    private final RiskAssessor assessor = new RiskAssessor();

    private RiskPolicy policy() {
        return new RiskPolicy(
                "policy-test",
                70,
                90,
                2,
                6,
                Duration.ofDays(3),
                Duration.ofMinutes(15),
                Duration.ofHours(1),
                7,
                14,
                0.95
        );
    }

    private ResolvedProfile profile(ResolvedProfile.Statistic... statistics) {
        return profile(false, statistics);
    }

    private ResolvedProfile profile(boolean stale, ResolvedProfile.Statistic... statistics) {
        Map<String, ResolvedProfile.Statistic> byKey = new LinkedHashMap<>();
        for (ResolvedProfile.Statistic statistic : statistics) {
            byKey.put(
                    ResolvedProfile.statisticKey(
                            statistic.metricName(),
                            statistic.applianceType(),
                            statistic.weekdayGroup(),
                            statistic.timeBucket()),
                    statistic);
        }
        return new ResolvedProfile(
                "profile-1",
                LocalDate.of(2026, 9, 20),
                NOW.minusHours(6),
                stale,
                List.of(),
                Map.copyOf(byKey)
        );
    }

    /** 시간대별 통계 한 줄. weekday_group은 배치가 쓰는 ALL로 둔다. */
    private ResolvedProfile.Statistic statistic(
            String timeBucket,
            double p50,
            double mad,
            long sampleCount,
            long eligibleDayCount
    ) {
        return new ResolvedProfile.Statistic(
                ACTIVITY,
                null,
                "ALL",
                timeBucket,
                sampleCount,
                eligibleDayCount,
                p50,
                p50,
                mad,
                "count",
                "READY"
        );
    }

    /** 평소 12:00까지 다섯 번 켰고 변동이 없다. */
    private ResolvedProfile.Statistic fiveStartsByNoon(long eligibleDayCount) {
        return statistic(BUCKET, 5, 0, 20, eligibleDayCount);
    }

    /** 오늘을 빠짐없이 봤고 방금 전에도 보고 있는 관측 품질. */
    private ObservationQuality observed(OffsetDateTime lastObservedAt) {
        long covered = Duration.between(DAY_START, lastObservedAt).getSeconds();
        return new ObservationQuality(
                lastObservedAt,
                true,
                DAY_START.minusDays(2),
                Map.of(TODAY, Math.max(0, covered), TODAY.minusDays(1), 86_400L));
    }

    /** 끝난 유효 사용 한 건. */
    private ActivityLedger.Use use(String applianceType, OffsetDateTime from, OffsetDateTime to) {
        return new ActivityLedger.Use(
                applianceType, from, to, false, from.atZoneSameInstant(KST).toLocalDate());
    }

    /** 오늘 {@code hour}시에 3분 쓴 전기포트. */
    private ActivityLedger.Use kettleAt(int hour) {
        return use("KETTLE", DAY_START.plusHours(hour), DAY_START.plusHours(hour).plusMinutes(3));
    }

    private CurrentState state(
            boolean away,
            OffsetDateTime lastObservedAt,
            ActivityLedger.Use... uses
    ) {
        return new CurrentState(
                NOW, away, observed(lastObservedAt), ActivityLedger.of(List.of(uses)));
    }

    private IndicatorResult indicator(RiskAssessment assessment, String code) {
        return assessment.indicators().stream()
                .filter(result -> code.equals(result.code()))
                .findFirst()
                .orElseThrow();
    }

    @Test
    void 활동이_평소보다_적으면_참고_점수만_내고_등급은_내지_않는다() {
        // 12:00까지 한 번 켰다. 평소는 다섯 번이다.
        var assessment = assessor.assess(
                Optional.of(profile(fiveStartsByNoon(20))),
                state(false, NOW, kettleAt(7)),
                policy());

        // z = (5-1)/1 = 4, clip((4-2)/4) = 0.5
        assertThat(assessment.status()).isEqualTo(AssessmentStatus.VALID);
        assertThat(assessment.score()).isEqualTo(50);
        // 등급은 분석 서비스 이벤트가 정한다. 자체 평가는 등급을 내지 않는다.
        assertThat(assessment.level()).isNull();
        assertThat(assessment.indicators()).extracting(IndicatorResult::code).containsExactly("A");
    }

    @Test
    void 점수가_100이어도_등급은_내지_않는다() {
        // 평소 여덟 번 켜는 시각인데 한 번도 켜지 않았다. z = 8 >= zHigh 6
        var assessment = assessor.assess(
                Optional.of(profile(statistic(BUCKET, 8, 0, 20, 20))),
                state(false, NOW),
                policy());

        assertThat(indicator(assessment, "A").score()).isEqualTo(1.0);
        assertThat(assessment.score()).isEqualTo(100);
        assertThat(assessment.level()).isNull();
    }

    @Test
    void MAD가_0이어도_최소_변동폭_1회로_나눈다() {
        var assessment = assessor.assess(
                Optional.of(profile(fiveStartsByNoon(20))),
                state(false, NOW),
                policy());

        assertThat(indicator(assessment, "A").compareSpread()).isEqualTo(1.0);
    }

    @Test
    void z가_하한_이하면_0점이다() {
        // 세 번 켰다. z = (5-3)/1 = 2 = zLow
        var assessment = assessor.assess(
                Optional.of(profile(fiveStartsByNoon(20))),
                state(false, NOW, kettleAt(7), kettleAt(8), kettleAt(9)),
                policy());

        assertThat(indicator(assessment, "A").score()).isZero();
        assertThat(assessment.score()).isZero();
    }

    @Test
    void 외출_중에는_지표를_제외하고_점수를_내지_않는다() {
        var assessment = assessor.assess(
                Optional.of(profile(fiveStartsByNoon(20))),
                state(true, NOW),
                policy());

        assertThat(assessment.status()).isEqualTo(AssessmentStatus.PARTIAL);
        assertThat(assessment.score()).isNull();
        assertThat(assessment.level()).isNull();
        assertThat(indicator(assessment, "A").excludedReason()).isEqualTo("AWAY");
    }

    @Test
    void 관측이_끊기면_활동_감소를_계산하지_않는다() {
        // 마지막 관측이 4시간 전이다. 켜지 않은 것이 아니라 켰는지 볼 수 없었다.
        var assessment = assessor.assess(
                Optional.of(profile(fiveStartsByNoon(20))),
                state(false, NOW.minusHours(4)),
                policy());

        assertThat(indicator(assessment, "A").excludedReason()).isEqualTo("OBSERVATION_STALE");
        assertThat(assessment.status()).isEqualTo(AssessmentStatus.PARTIAL);
        assertThat(assessment.score()).isNull();
    }

    @Test
    void 공백_뒤_스냅샷_하나로는_활동_부족을_확정하지_않는다() {
        // 방금 스냅샷이 도착해 신선도는 회복됐지만 오늘의 절반만 봤다.
        // 그 한 건은 공백 동안 무슨 일이 있었는지 말해 주지 않는다.
        ObservationQuality halfSeen = new ObservationQuality(
                NOW,
                true,
                NOW.minusMinutes(1),
                Map.of(TODAY, Duration.between(DAY_START, NOW).getSeconds() / 2));
        var assessment = assessor.assess(
                Optional.of(profile(fiveStartsByNoon(20))),
                new CurrentState(NOW, false, halfSeen, ActivityLedger.of(List.of())),
                policy());

        assertThat(indicator(assessment, "A").excludedReason())
                .isEqualTo("OBSERVATION_COVERAGE");
        assertThat(assessment.status()).isEqualTo(AssessmentStatus.PARTIAL);
        assertThat(assessment.score()).isNull();
    }

    @Test
    void 커버리지를_집계하지_않는_옛_입력은_활동_감소를_계산하지_않는다() {
        // 옛 입력 계약은 마지막 관측 시각만 싣는다. 모르는 구간을 정상 관측으로 바꾸지 않는다.
        var assessment = assessor.assess(
                Optional.of(profile(fiveStartsByNoon(20))),
                new CurrentState(NOW, false, NOW, EVAL.minusSeconds(5500), false, Map.of()),
                policy());

        assertThat(indicator(assessment, "A").excludedReason()).isEqualTo("USAGE_UNVERIFIED");
        assertThat(assessment.status()).isEqualTo(AssessmentStatus.PARTIAL);
        assertThat(assessment.score()).isNull();
    }

    @Test
    void 프로필이_없으면_학습_중으로_보고_점수를_내지_않는다() {
        var assessment = assessor.assess(Optional.empty(), state(false, NOW), policy());

        assertThat(assessment.status()).isEqualTo(AssessmentStatus.LEARNING);
        assertThat(assessment.score()).isNull();
        assertThat(assessment.level()).isNull();
    }

    @Test
    void 유효기간을_넘긴_프로필로는_평가하지_않는다() {
        var assessment = assessor.assess(
                Optional.of(profile(true, fiveStartsByNoon(20))),
                state(false, NOW),
                policy());

        assertThat(assessment.status()).isEqualTo(AssessmentStatus.INSUFFICIENT_DATA);
        assertThat(assessment.score()).isNull();
    }

    @Test
    void 표본이_모자란_통계는_믿지_않는다() {
        var assessment = assessor.assess(
                // 최소 표본 7에 못 미친다.
                Optional.of(profile(statistic(BUCKET, 5, 0, 3, 3))),
                state(false, NOW),
                policy());

        assertThat(indicator(assessment, "A").excludedReason()).isEqualTo("INSUFFICIENT_SAMPLE");
        assertThat(assessment.status()).isEqualTo(AssessmentStatus.PARTIAL);
        assertThat(assessment.score()).isNull();
    }

    @Test
    void 비교_통계가_없으면_점수를_내지_않는다() {
        // 누락 지표를 0으로 채우면 "평소와 같음"으로 보이므로 점수를 비운다.
        var assessment = assessor.assess(Optional.of(profile()), state(false, NOW), policy());

        assertThat(indicator(assessment, "A").excludedReason()).isEqualTo("NO_STATISTIC");
        assertThat(assessment.status()).isEqualTo(AssessmentStatus.PARTIAL);
        assertThat(assessment.score()).isNull();
    }

    @Test
    void 신뢰도가_낮아도_점수를_깎지_않는다() {
        var manyDays = assessor.assess(
                Optional.of(profile(fiveStartsByNoon(28))), state(false, NOW), policy());
        var fewDays = assessor.assess(
                Optional.of(profile(fiveStartsByNoon(7))), state(false, NOW), policy());

        assertThat(manyDays.confidence()).isEqualTo(1.0);
        assertThat(fewDays.confidence()).isEqualTo(0.5);
        // 신뢰도는 점수에 곱하지 않는다. 데이터 부족이 낮은 위험으로 둔갑하면 안 된다.
        // 한 번도 켜지 않았다. z = 5, clip((5-2)/4) = 0.75
        assertThat(fewDays.score()).isEqualTo(manyDays.score()).isEqualTo(75);
    }

    @Test
    void 아직_오지_않은_구간의_통계는_쓰지_않는다() {
        // 12:10에 비교하는 구간은 12:30이 아니라 12:00이다.
        var assessment = assessor.assess(
                Optional.of(profile(
                        fiveStartsByNoon(20),
                        statistic("12:30", 50, 0, 20, 20))),
                state(false, NOW),
                policy());

        assertThat(indicator(assessment, "A").bucket()).isEqualTo(BUCKET);
        assertThat(indicator(assessment, "A").compareCenter()).isEqualTo(5.0);
    }

    @Test
    void 활동_감소는_기준_시각_전에_시작한_사용만_센다() {
        var assessment = assessor.assess(
                Optional.of(profile(fiveStartsByNoon(20))),
                state(false, NOW,
                        kettleAt(7),
                        // 12:05 시작. 기준 시각 12:00에는 아직 일어나지 않았다.
                        use("MICROWAVE", NOW.minusMinutes(5), NOW.minusMinutes(2))),
                policy());

        IndicatorResult activity = indicator(assessment, "A");
        assertThat(activity.observed()).isEqualTo(1.0);
        assertThat(activity.bucket()).isEqualTo(BUCKET);
        // z = (5-1)/1 = 4, clip((4-2)/4) = 0.5
        assertThat(activity.score()).isEqualTo(0.5);
    }

    @Test
    void 자정을_넘겨_이어진_사용은_새_날의_시작으로_세지_않는다() {
        // 어제 23:50에 시작해 오늘 00:20에 끝난 사용 하나뿐이다.
        ActivityLedger.Use overnight = use(
                "IRON", DAY_START.minusMinutes(10), DAY_START.plusMinutes(20));
        var assessment = assessor.assess(
                Optional.of(profile(fiveStartsByNoon(20))),
                state(false, NOW, overnight),
                policy());

        // 시작은 어제에만 든다. 오늘의 누적 시작은 0이다.
        assertThat(indicator(assessment, "A").observed()).isZero();
    }

    @Test
    void 자정_직후에는_전날의_24시_구간과_비교한다() {
        OffsetDateTime justAfterMidnight = OffsetDateTime.parse("2026-09-21T00:10:00+09:00");
        OffsetDateTime yesterdayStart = DAY_START.minusDays(1);
        ObservationQuality quality = new ObservationQuality(
                justAfterMidnight,
                true,
                yesterdayStart.minusDays(1),
                Map.of(TODAY.minusDays(1), 86_400L, TODAY, 600L));
        var assessment = assessor.assess(
                Optional.of(profile(statistic("24:00", 5, 0, 20, 20))),
                new CurrentState(justAfterMidnight, false, quality, ActivityLedger.of(List.of(
                        use("KETTLE", yesterdayStart.plusHours(8), yesterdayStart.plusHours(8)
                                .plusMinutes(5))))),
                policy());

        IndicatorResult activity = indicator(assessment, "A");
        // 기준 시각은 오늘 자정, 곧 전날의 마지막 구간이다.
        assertThat(activity.bucket()).isEqualTo("24:00");
        // 전날 하루치 누적과 전날 하루치 분포를 비교한다.
        assertThat(activity.observed()).isEqualTo(1.0);
        assertThat(activity.score()).isEqualTo(0.5);
    }
}

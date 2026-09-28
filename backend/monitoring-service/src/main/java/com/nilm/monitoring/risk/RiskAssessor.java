package com.nilm.monitoring.risk;

import com.nilm.monitoring.service.ResolvedProfile;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * 가구 프로필과 현재 상태를 비교해 참고 점수를 내는 순수 계산기.
 *
 * <p>Spring도 JPA도 쓰지 않는다. 입력으로 받은 값만 보고 같은 입력에는 언제나 같은 답을 낸다.
 * 점수식은 설계 11.2절의 "미검증 정책안"이고, 임계·변동폭은 모두 {@link RiskPolicy}에서 온다.
 *
 * <p>계산하는 지표는 활동 감소 A 하나다. 장시간 무활동(I)·장시간 사용(U)은 "정해진 시간이
 * 지났는데 무엇이 일어나지 않았다/계속된다"를 실시간으로 보는 판단이고, 그 소유자는 분석
 * 서비스다. PROLONGED_INACTIVITY·PROLONGED_APPLIANCE_USE 이벤트가 오면 모니터링은 이벤트
 * 등급 슬롯으로만 반영한다. 같은 사건을 두 주체가 각자 판단하면 알림이 겹치고 최종 등급의
 * 주체가 둘로 갈린다. 루틴 미사용(M)은 가구 기준선을 써야 하는 판단이라 모니터링에서
 * 점수화하지 않고, ROUTINE_MISSED 이벤트도 SHADOW로 저장·표시만 한다.
 *
 * <p>A는 "오늘 평소보다 덜 움직였다"는 약한 신호라서 등급을 내지 않는다. 점수는 화면과
 * 보고서의 참고값으로만 쓰고, 등급과 알림은 이벤트 경로가 정한다.
 *
 * <p>비교 기준 시각은 {@link EvaluationPoint}가 정한다. 프로필 통계는 구간 끝 시각에서 잰
 * 값이므로 현재 관측값도 같은 시각에서 재야 한다. 12:10에 12:30 통계를 꺼내 쓰면 아직 오지
 * 않은 20분치 활동을 이미 했어야 하는 것으로 비교하게 된다.
 *
 * <p>지표는 계산에 앞서 입력 품질을 먼저 본다. 보지 못한 구간은 "아무 일도 없었던 구간"이
 * 아니다. 신선도가 떨어졌거나 그 구간을 실제로 보지 못했으면 지표를 제외한다.
 * 제외는 0점이 아니다.
 */
public class RiskAssessor {

    /** 점수식의 판. 지표 구성이 바뀌면 이 값을 올려 과거 이력과 구분한다. */
    public static final String SCORE_VERSION = "monitoring-score-v2-A";

    /** 영업일과 시간대 구간의 기준. 프로필을 만든 배치와 같은 시간대를 쓴다. */
    private static final ZoneId ZONE = ZoneId.of("Asia/Seoul");

    private static final String METRIC_ACTIVITY = "CUMULATIVE_ACTIVITY_START_COUNT";

    /** MAD를 표준편차 눈금으로 옮기는 상수. */
    private static final double MAD_TO_SIGMA = 1.4826;

    /**
     * 활동 감소(A)의 최소 변동폭. 표본이 작거나 MAD가 0이어도 분모가 0이 되지 않게
     * 횟수의 최소 변동폭을 1회로 고정한다.
     */
    private static final double MIN_SPREAD_COUNT = 1.0;

    private static final String QUALITY_READY = "READY";
    private static final String GROUP_ALL = "ALL";

    private static final String REASON_AWAY = "AWAY";
    private static final String REASON_OBSERVATION_STALE = "OBSERVATION_STALE";
    /** 비교하려는 구간을 충분히 보지 못했다. 사용 부재를 확정할 수 없다. */
    private static final String REASON_OBSERVATION_COVERAGE = "OBSERVATION_COVERAGE";
    /** 입력이 Gold의 유효 사용 정의를 통과하지 않은 숫자다. 같은 뜻으로 비교할 수 없다. */
    private static final String REASON_USAGE_UNVERIFIED = "USAGE_UNVERIFIED";
    private static final String REASON_NO_STATISTIC = "NO_STATISTIC";
    private static final String REASON_INSUFFICIENT_SAMPLE = "INSUFFICIENT_SAMPLE";

    /**
     * 평가 한 번. 결과의 등급은 언제나 비어 있다.
     *
     * @param profile 이번 평가가 쓸 프로필. 비어 있으면 아직 학습 기간이다
     * @param state 평가 시작 시점에 고정한 현재 상태
     */
    public RiskAssessment assess(
            Optional<ResolvedProfile> profile,
            CurrentState state,
            RiskPolicy policy
    ) {
        if (profile.isEmpty()) {
            return unavailable(AssessmentStatus.LEARNING, null, policy);
        }
        ResolvedProfile resolved = profile.get();
        if (resolved.stale()) {
            // 유효기간을 넘긴 프로필로 비교하면 근거 없는 점수가 나온다.
            return unavailable(
                    AssessmentStatus.INSUFFICIENT_DATA, resolved.profileVersion(), policy);
        }

        // 프로필 통계와 같은 시각에서 현재 값을 잰다.
        EvaluationPoint point = EvaluationPoint.of(state.now(), ZONE);

        // 스냅샷이 끊긴 동안의 활동 부족은 대상자가 가만히 있었다는 뜻이 아니라
        // 우리가 보지 못했다는 뜻이다. 현재 값을 쓰는 지표를 아예 계산하지 않는다.
        boolean observationStale = isObservationStale(state, policy);

        Computed activity = assessActivityDrop(resolved, state, policy, point, observationStale);
        List<IndicatorResult> indicators = List.of(activity.result());

        if (!activity.result().included()) {
            // 제외된 지표를 0으로 채우면 데이터 없음이 "정상"으로 보인다. 점수를 내지 않는다.
            return new RiskAssessment(
                    null, null, AssessmentStatus.PARTIAL, 0.0, indicators,
                    resolved.profileVersion(), policy.policyVersion(), SCORE_VERSION);
        }

        int score = (int) Math.round(100 * activity.result().score());

        // 신뢰도는 점수에 곱하지 않는다. 데이터 부족을 낮은 위험으로 바꾸지 않기 위해서다.
        double confidence =
                Math.min(1.0, (double) activity.eligibleDays() / policy.minEligibleDays());

        return new RiskAssessment(
                score, null, AssessmentStatus.VALID, confidence, indicators,
                resolved.profileVersion(), policy.policyVersion(), SCORE_VERSION);
    }

    /**
     * 활동 감소 A.
     *
     * <p>영업일이 시작한 뒤 기준 시각까지의 유효 사용 시작 횟수를 과거 같은 시각까지의
     * 누적 분포와 비교한다. 12:10에 12:30까지의 누적 분포와 견주면 아직 오지 않은 20분치
     * 활동이 모자란 것으로 보인다. 그래서 관측값도 분포도 같은 구간 경계에서 읽는다.
     *
     * <p>보지 못한 구간이 있으면 시작 횟수는 실제보다 적게 세어진다. 적을수록 위험한
     * 지표라서 그대로 비교하면 관측 공백이 곧 위험이 된다. 커버리지가 모자라면 제외한다.
     */
    private Computed assessActivityDrop(
            ResolvedProfile profile,
            CurrentState state,
            RiskPolicy policy,
            EvaluationPoint point,
            boolean observationStale
    ) {
        if (state.away()) {
            return Computed.excluded("A", REASON_AWAY);
        }
        if (observationStale) {
            return Computed.excluded("A", REASON_OBSERVATION_STALE);
        }

        OptionalInt starts = state.activity().startCount(point.businessDate(), point.at());
        if (starts.isEmpty()) {
            return Computed.excluded("A", REASON_USAGE_UNVERIFIED);
        }
        if (!covered(state, policy, point.businessDate(), point.businessDayStart(), point.at())) {
            return Computed.excluded("A", REASON_OBSERVATION_COVERAGE);
        }

        // 활동이 적을수록 위험하므로 부호가 반대다.
        return compare("A", profile, policy, point, METRIC_ACTIVITY,
                starts.getAsInt(), MIN_SPREAD_COUNT, true);
    }

    /** 현재 값 하나를 시간대별 통계 한 줄과 비교해 0~1 점수로 옮긴다. */
    private Computed compare(
            String code,
            ResolvedProfile profile,
            RiskPolicy policy,
            EvaluationPoint point,
            String metricName,
            double observed,
            double minSpread,
            boolean lowerIsWorse
    ) {
        ResolvedProfile.Statistic statistic = lookup(profile, metricName, point);
        if (statistic == null || statistic.p50() == null) {
            return Computed.excluded(code, REASON_NO_STATISTIC);
        }
        if (statistic.sampleCount() < policy.minSampleCount()
                || !QUALITY_READY.equals(statistic.qualityStatus())) {
            return Computed.excluded(code, REASON_INSUFFICIENT_SAMPLE);
        }

        double center = statistic.p50();
        double mad = statistic.mad() == null ? 0 : statistic.mad();
        // 표본이 작거나 MAD가 0이어도 분모가 0이 되지 않게 바닥을 깐다.
        double spread = Math.max(MAD_TO_SIGMA * mad, minSpread);
        double z = Math.max(0, (lowerIsWorse ? center - observed : observed - center) / spread);
        double score = clip((z - policy.zLow()) / (policy.zHigh() - policy.zLow()));

        String bucket = statistic.timeBucket() == null ? point.bucket() : statistic.timeBucket();
        return new Computed(
                new IndicatorResult(code, score, observed, center, spread, bucket, null),
                statistic.eligibleDayCount());
    }

    /**
     * 비교할 통계 한 줄을 찾는다.
     *
     * <p>요일 그룹은 좁은 조건부터 넓은 조건으로 완화한다(설계 10장).
     * 시간대 구간은 배치가 쓰는 표기 하나만 본다. 표기가 어긋나면 조용히 다른 구간을
     * 집어오는 대신 지표를 제외하는 편이 낫다.
     */
    private ResolvedProfile.Statistic lookup(
            ResolvedProfile profile,
            String metricName,
            EvaluationPoint point
    ) {
        for (String group : List.of(weekdayGroup(point.businessDate()), GROUP_ALL)) {
            Optional<ResolvedProfile.Statistic> found =
                    profile.statistic(metricName, null, group, point.bucket());
            if (found.isPresent()) {
                return found.get();
            }
        }
        return null;
    }

    /**
     * 그 영업일 구간을 실제로 충분히 봤는가.
     *
     * <p>커버리지를 집계하지 않는 입력은 "다 봤다"가 아니라 "모른다"로 다룬다.
     */
    private boolean covered(
            CurrentState state,
            RiskPolicy policy,
            LocalDate date,
            OffsetDateTime windowStart,
            OffsetDateTime until
    ) {
        ObservationQuality observation = state.observation();
        if (!observation.coverageTracked()) {
            return false;
        }
        return observation.coverageRatio(date, windowStart, until)
                >= policy.minObservationCoverage();
    }

    private boolean isObservationStale(CurrentState state, RiskPolicy policy) {
        OffsetDateTime lastObservedAt = state.observation().lastObservedAt();
        if (lastObservedAt == null) {
            return true;
        }
        return state.now().toEpochSecond() - lastObservedAt.toEpochSecond()
                > policy.observationMaxAge().getSeconds();
    }

    private String weekdayGroup(LocalDate businessDate) {
        DayOfWeek day = businessDate.getDayOfWeek();
        return day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY ? "WEEKEND" : "WEEKDAY";
    }

    private static double clip(double value) {
        if (value < 0) {
            return 0;
        }
        return Math.min(value, 1);
    }

    private RiskAssessment unavailable(
            AssessmentStatus status,
            String profileVersion,
            RiskPolicy policy
    ) {
        return new RiskAssessment(
                null, null, status, 0.0, List.of(),
                profileVersion, policy.policyVersion(), SCORE_VERSION);
    }

    /**
     * 지표 하나의 계산 결과와, 신뢰도를 매기는 데 쓸 관측일 수.
     * 관측일 수는 이력에 남기지 않으므로 결과 레코드 밖에 둔다.
     */
    private record Computed(IndicatorResult result, long eligibleDays) {

        static Computed excluded(String code, String reason) {
            return new Computed(IndicatorResult.excluded(code, reason), 0);
        }
    }
}

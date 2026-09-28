package com.nilm.monitoring.service;

import com.nilm.monitoring.config.enums.StateChangeTrigger;
import com.nilm.monitoring.domain.HouseholdProfile;
import com.nilm.monitoring.domain.HouseholdProfileStatistic;
import com.nilm.monitoring.domain.HouseholdRoutineBaseline;
import com.nilm.monitoring.domain.Subject;
import com.nilm.monitoring.dto.SubjectProfileResponse;
import com.nilm.monitoring.dto.kafka.HouseholdProfileMessage;
import com.nilm.monitoring.repository.HouseholdProfileRepository;
import com.nilm.monitoring.repository.HouseholdProfileStatisticRepository;
import com.nilm.monitoring.repository.HouseholdRoutineBaselineRepository;
import com.nilm.monitoring.repository.SubjectRepository;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Comparator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * gold.household-profile.v1로 들어오는 가구 프로필을 버전 단위로 쌓고,
 * 평가에 쓸 최신 한 벌을 골라 준다.
 *
 * <p>여기서는 점수도 등급도 계산하지 않는다. 평가 로직은
 * {@link #resolveActive(String, OffsetDateTime)}가 돌려주는 값 한 벌만 가져간다.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class HouseholdProfileService {

    /** 배치의 영업일 기준. 날짜 비교는 모두 이 시간대로 맞춘다. */
    private static final ZoneId ZONE = ZoneId.of("Asia/Seoul");

    /** 배치가 평가에 써도 좋다고 선언한 품질. */
    private static final String QUALITY_READY = "READY";

    private static final String QUALITY_UNKNOWN = "UNKNOWN";
    private static final String DELIVERY_ACTIVE = "ACTIVE";
    private static final String DELIVERY_SHADOW = "SHADOW";

    /** 가전별 대표 기준선의 범위. 담당자 화면에는 이것만 보여 준다. */
    private static final String SCOPE_OVERALL = "OVERALL";

    private final SubjectRepository subjects;
    private final HouseholdProfileRepository profiles;
    private final HouseholdRoutineBaselineRepository baselines;
    private final HouseholdProfileStatisticRepository statistics;
    private final SubjectAccessGuard accessGuard;

    /**
     * 새 프로필이 들어오면 곧바로 다시 평가한다.
     *
     * <p>평가 쪽이 이 서비스의 {@link #resolveActive(String, OffsetDateTime)}를 쓰므로
     * 서로를 가리키는 모양이 된다. 생성 시점에 묶지 않고 필요할 때 꺼내 고리를 끊는다.
     */
    private final ObjectProvider<RiskAssessmentService> assessments;

    private final ApplicationEventPublisher publisher;

    /**
     * 프로필 유효기간. 넘겼다고 버리지 않고 {@code ResolvedProfile.stale}로 표시만 한다.
     * 얼마나 오래된 프로필까지 믿을지는 평가 정책이 정할 일이다.
     */
    @Value("${app.profile.max-age-days:3}")
    private long maxAgeDays;

    /**
     * 프로필 한 건을 받아 반영한다.
     *
     * <p>어떤 경로로도 예외를 던지지 않는다. 컨슈머의 오류 핸들러가 컨테이너를 멈추기 때문에,
     * 한 가구의 잘못된 프로필이 전체 수신을 막으면 안 된다.
     */
    @Transactional
    public void receive(HouseholdProfileMessage message) {
        if (!isProcessable(message)) {
            return;
        }

        // 프로필 토픽에는 이 서비스가 모르는 가구도 흘러온다. 조용히 건너뛴다.
        if (!subjects.existsByHouseholdId(message.householdId())) {
            log.debug("등록되지 않은 가구의 프로필 무시: householdId={}, profileVersion={}",
                    message.householdId(), message.profileVersion());
            return;
        }

        // ACTIVE 교체는 읽고 쓰는 일이라 가구 행 락을 먼저 잡는다.
        // 같은 가구의 프로필이 겹쳐 들어와도 한 번에 한 트랜잭션만 판정하게 된다.
        List<Subject> matches = subjects.findHouseholdForUpdate(message.householdId());
        if (matches.size() != 1) {
            log.warn("가구에 대상자가 정확히 한 명이 아니어서 프로필을 건너뛴다: householdId={}, 수={}",
                    message.householdId(), matches.size());
            return;
        }
        Subject subject = matches.get(0);

        // 재전송이나 파티션 재배치로 같은 버전이 다시 와도 아무것도 하지 않는다.
        // 정상 SHADOW -> ACTIVE는 배치의 새 실행/버전으로 와야 하므로 이 멱등 판정에
        // 막히지 않는다. 같은 버전의 mode만 바꿔 보내는 것은 계약 위반이다.
        if (profiles.existsByHouseholdIdAndProfileVersion(
                message.householdId(), message.profileVersion())) {
            log.debug("이미 수신한 프로필 버전 무시: householdId={}, profileVersion={}",
                    message.householdId(), message.profileVersion());
            return;
        }

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

        String rejection = rejectionReason(message);
        if (rejection != null) {
            // 품질이 모자란 프로필은 본문을 남기지 않는다. 왜 반영되지 않았는지만 기록한다.
            profiles.save(header(message, now, HouseholdProfile.Status.REJECTED, rejection));
            log.info("품질 미달 프로필을 REJECTED로 기록: householdId={}, profileVersion={}, 사유={}",
                    message.householdId(), message.profileVersion(), rejection);
            return;
        }

        if (DELIVERY_SHADOW.equals(message.deliveryMode())) {
            HouseholdProfile stored = profiles.save(
                    header(message, now, HouseholdProfile.Status.SHADOW, null));
            saveDetails(stored.getId(), message);
            log.info("SHADOW 프로필을 운영 반영 없이 저장: householdId={}, profileVersion={}, revision={}",
                    message.householdId(), message.profileVersion(), message.profileRevision());
            return;
        }

        List<HouseholdProfile> active = profiles
                .findByHouseholdIdAndStatusOrderByEffectiveFromDescIdDesc(
                        message.householdId(), HouseholdProfile.Status.ACTIVE);

        // 날짜가 우선이고, 같은 날짜에서는 생산자가 부여한 revision만 비교한다.
        // UUID 문자열이나 Kafka 수신 순서는 절대로 최신성 근거로 쓰지 않는다.
        boolean older = active.stream()
                .anyMatch(current -> compareOrder(message, current) <= 0);
        if (older) {
            HouseholdProfile stored = profiles.save(
                    header(message, now, HouseholdProfile.Status.SUPERSEDED, null));
            saveDetails(stored.getId(), message);
            log.info("현재 ACTIVE보다 오래된 프로필을 이력으로만 저장: householdId={}, profileVersion={}, asOfDate={}",
                    message.householdId(), message.profileVersion(), message.asOfDate());
            return;
        }

        // 미래 후보를 ACTIVE로 바꾸면 지금 평가 가능한 프로필이 사라진다. PENDING으로
        // 보존하고 resolveActive가 평가 시각에 발효된 후보 중 최신을 선택하게 한다.
        if (message.effectiveFrom().isAfter(now)) {
            HouseholdProfile stored = profiles.save(
                    header(message, now, HouseholdProfile.Status.PENDING, null));
            saveDetails(stored.getId(), message);
            log.info("미래 적용 프로필을 PENDING으로 저장: householdId={}, profileVersion={}, effectiveFrom={}",
                    message.householdId(), message.profileVersion(), message.effectiveFrom());
            return;
        }

        HouseholdProfile stored = profiles.save(
                header(message, now, HouseholdProfile.Status.ACTIVE, null));
        saveDetails(stored.getId(), message);
        active.forEach(HouseholdProfile::supersede);

        // 대시보드가 갱신 사실을 알아차릴 수 있도록 상태 버전을 올린다.
        subject.touch(now);
        publisher.publishEvent(
                new SubjectStateChanged(subject.getId(), StateChangeTrigger.PROFILE_UPDATED));
        log.info("최신 프로필 반영: householdId={}, profileVersion={}, asOfDate={}",
                message.householdId(), message.profileVersion(), message.asOfDate());

        // 비교 기준이 통째로 바뀌었다. 다음 타이머를 기다리지 않고 바로 다시 평가한다.
        assessments.getObject().evaluate(
                message.householdId(), now, StateChangeTrigger.PROFILE_UPDATED);
    }

    /**
     * 평가에 쓸 프로필을 고른다.
     *
     * @param evaluationTime 평가 기준 시각
     * @return 쓸 수 있는 프로필 한 벌. 조건을 하나라도 못 채우면 비어 있다.
     */
    @Transactional(readOnly = true)
    public Optional<ResolvedProfile> resolveActive(
            String householdId,
            OffsetDateTime evaluationTime
    ) {
        if (householdId == null || householdId.isBlank() || evaluationTime == null) {
            return Optional.empty();
        }

        List<HouseholdProfile> candidates = operationalCandidates(householdId).stream()
                .filter(profile -> !profile.getEffectiveFrom().isAfter(evaluationTime))
                .sorted(profileOrder().reversed())
                .toList();
        if (candidates.isEmpty()) {
            return Optional.empty();
        }
        HouseholdProfile profile = candidates.get(0);

        // "오늘은 어제까지의 데이터로 만든 프로필로 평가한다"는 규칙.
        // 배치가 넣는 effective_from은 실행 시각이라 믿지 않고 as_of_date로 직접 검사한다.
        LocalDate evaluationDate = evaluationTime.atZoneSameInstant(ZONE).toLocalDate();
        if (!profile.getAsOfDate().isBefore(evaluationDate)) {
            log.warn("평가일 당일 이후 데이터로 만든 프로필이라 쓰지 않는다: householdId={}, asOfDate={}, 평가일={}",
                    householdId, profile.getAsOfDate(), evaluationDate);
            return Optional.empty();
        }

        long ageDays = ChronoUnit.DAYS.between(profile.getAsOfDate(), evaluationDate);
        boolean stale = ageDays > maxAgeDays;
        if (stale) {
            log.warn("유효기간을 넘긴 프로필: householdId={}, asOfDate={}, 경과일={}, 허용={}",
                    householdId, profile.getAsOfDate(), ageDays, maxAgeDays);
        }

        return Optional.of(new ResolvedProfile(
                profile.getProfileVersion(),
                profile.getAsOfDate(),
                profile.getEffectiveFrom(),
                stale,
                resolveBaselines(profile.getId()),
                resolveStatistics(profile.getId())
        ));
    }

    /**
     * 담당자 화면이 현재 반영된 프로필을 확인하는 조회.
     * 가전별 OVERALL 기준선만 추려 보여 준다.
     *
     * @return ACTIVE 프로필이 없으면 비어 있다.
     */
    @Transactional(readOnly = true)
    public Optional<SubjectProfileResponse> getProfile(String authSub, Long subjectId) {
        Subject subject = accessGuard.requireReadable(authSub, subjectId);

        List<HouseholdProfile> active = operationalCandidates(subject.getHouseholdId()).stream()
                .filter(profile -> !profile.getEffectiveFrom().isAfter(OffsetDateTime.now(ZoneOffset.UTC)))
                .sorted(profileOrder().reversed())
                .toList();
        if (active.isEmpty()) {
            return Optional.empty();
        }
        HouseholdProfile profile = active.get(0);

        List<SubjectProfileResponse.ApplianceBaseline> appliances = baselines
                .findByProfileIdOrderByApplianceTypeAscBaselineScopeAscWeekdayAsc(profile.getId())
                .stream()
                .filter(baseline -> SCOPE_OVERALL.equals(baseline.getBaselineScope()))
                .map(baseline -> new SubjectProfileResponse.ApplianceBaseline(
                        baseline.getApplianceType(),
                        baseline.getSampleDays(),
                        baseline.getActiveDays(),
                        baseline.getDailyUseProbability(),
                        baseline.getReliabilityWeight(),
                        baseline.getFirstUseTimeP50Second(),
                        baseline.getExpectedUntilSecond(),
                        baseline.getPreferredWindowStartSecond(),
                        baseline.getPreferredWindowEndSecond(),
                        baseline.getQualityStatus(),
                        baseline.isEnabled()
                ))
                .toList();

        return Optional.of(new SubjectProfileResponse(
                String.valueOf(subject.getId()),
                profile.getProfileVersion(),
                profile.getAsOfDate(),
                profile.getWindowStartDate(),
                profile.getWindowEndDate(),
                profile.getEffectiveFrom(),
                profile.getPublishedAt(),
                profile.getReceivedAt(),
                profile.getQualityStatus(),
                profile.getRuleVersion(),
                profile.getStatisticRuleVersion(),
                appliances
        ));
    }

    private List<ResolvedProfile.RoutineBaseline> resolveBaselines(Long profileId) {
        return baselines
                .findByProfileIdOrderByApplianceTypeAscBaselineScopeAscWeekdayAsc(profileId)
                .stream()
                .map(baseline -> new ResolvedProfile.RoutineBaseline(
                        baseline.getApplianceType(),
                        baseline.getBaselineScope(),
                        baseline.getWeekday(),
                        baseline.getSampleDays(),
                        baseline.getActiveDays(),
                        baseline.getDailyUseProbability(),
                        baseline.getReliabilityWeight(),
                        baseline.getFirstUseTimeP50Second(),
                        baseline.getExpectedUntilSecond(),
                        baseline.getPreferredWindowStartSecond(),
                        baseline.getPreferredWindowEndSecond(),
                        baseline.getQualityStatus(),
                        baseline.isEnabled()
                ))
                .toList();
    }

    private Map<String, ResolvedProfile.Statistic> resolveStatistics(Long profileId) {
        Map<String, ResolvedProfile.Statistic> resolved = new LinkedHashMap<>();
        for (HouseholdProfileStatistic row : statistics.findByProfileId(profileId)) {
            resolved.put(
                    ResolvedProfile.statisticKey(
                            row.getMetricName(),
                            row.getApplianceType(),
                            row.getWeekdayGroup(),
                            row.getTimeBucket()),
                    new ResolvedProfile.Statistic(
                            row.getMetricName(),
                            row.getApplianceType(),
                            row.getWeekdayGroup(),
                            row.getTimeBucket(),
                            row.getSampleCount(),
                            row.getEligibleDayCount(),
                            row.getP50(),
                            row.getP90(),
                            row.getMad(),
                            row.getUnit(),
                            row.getQualityStatus()
                    ));
        }
        return Map.copyOf(resolved);
    }

    private HouseholdProfile header(
            HouseholdProfileMessage message,
            OffsetDateTime receivedAt,
            HouseholdProfile.Status status,
            String rejectionReason
    ) {
        return new HouseholdProfile(
                message.householdId(),
                message.profileVersion(),
                message.profileRevision(),
                message.deliveryMode(),
                message.asOfDate(),
                message.windowStartDate(),
                message.windowEndDate(),
                message.effectiveFrom(),
                message.publishedAt(),
                receivedAt,
                message.inputSnapshotId(),
                message.ruleVersion(),
                message.statisticRuleVersion(),
                blankToUnknown(message.qualityStatus()),
                status,
                rejectionReason
        );
    }

    private void saveDetails(Long profileId, HouseholdProfileMessage message) {
        saveBaselines(profileId, message.routineBaselines());
        saveStatistics(profileId, message.statistics());
    }

    /**
     * NULL이 섞인 열은 DB마다 유니크 비교가 달라 중복을 막지 못한다.
     * 같은 키가 두 번 오면 먼저 온 줄만 남긴다.
     */
    private void saveBaselines(
            Long profileId,
            List<HouseholdProfileMessage.RoutineBaseline> rows
    ) {
        if (rows == null || rows.isEmpty()) {
            return;
        }
        Set<String> seen = new HashSet<>();
        List<HouseholdRoutineBaseline> stored = new ArrayList<>();
        for (HouseholdProfileMessage.RoutineBaseline row : rows) {
            if (row == null || isBlank(row.applianceType()) || isBlank(row.baselineScope())) {
                continue;
            }
            String key = String.join("|",
                    row.applianceType(),
                    row.baselineScope(),
                    row.weekday() == null ? "" : row.weekday());
            if (!seen.add(key)) {
                log.warn("프로필 안에 중복된 기준선이 있어 건너뛴다: profileId={}, key={}", profileId, key);
                continue;
            }
            stored.add(new HouseholdRoutineBaseline(
                    profileId,
                    row.applianceType(),
                    row.baselineScope(),
                    row.weekday(),
                    row.sampleDays() == null ? 0 : row.sampleDays(),
                    row.activeDays() == null ? 0 : row.activeDays(),
                    row.dailyUseProbability(),
                    row.reliabilityWeight(),
                    row.firstUseTimeP50Second(),
                    row.expectedUntilSecond(),
                    row.preferredWindowStartSecond(),
                    row.preferredWindowEndSecond(),
                    blankToUnknown(row.qualityStatus()),
                    Boolean.TRUE.equals(row.enabled())
            ));
        }
        baselines.saveAll(stored);
    }

    private void saveStatistics(
            Long profileId,
            List<HouseholdProfileMessage.Statistic> rows
    ) {
        if (rows == null || rows.isEmpty()) {
            return;
        }
        Set<String> seen = new HashSet<>();
        List<HouseholdProfileStatistic> stored = new ArrayList<>();
        for (HouseholdProfileMessage.Statistic row : rows) {
            if (row == null || isBlank(row.metricName())) {
                continue;
            }
            // weekday_group은 NOT NULL이다. 배치가 생략하면 전체 구간을 뜻하는 ALL로 본다.
            String weekdayGroup = isBlank(row.weekdayGroup()) ? "ALL" : row.weekdayGroup();
            String key = ResolvedProfile.statisticKey(
                    row.metricName(), row.applianceType(), weekdayGroup, row.timeBucket());
            if (!seen.add(key)) {
                log.warn("프로필 안에 중복된 통계가 있어 건너뛴다: profileId={}, key={}", profileId, key);
                continue;
            }
            stored.add(new HouseholdProfileStatistic(
                    profileId,
                    row.metricName(),
                    row.applianceType(),
                    weekdayGroup,
                    row.timeBucket(),
                    row.sampleCount() == null ? 0L : row.sampleCount(),
                    row.eligibleDayCount() == null ? 0L : row.eligibleDayCount(),
                    row.p50(),
                    row.p90(),
                    row.mad(),
                    row.unit(),
                    blankToUnknown(row.qualityStatus())
            ));
        }
        statistics.saveAll(stored);
    }

    /**
     * 이 프로필을 평가에 쓰지 않을 이유.
     *
     * @return 쓸 수 있으면 null
     */
    private String rejectionReason(HouseholdProfileMessage message) {
        if (isBlank(message.qualityStatus())) {
            return "품질 상태가 비어 있음";
        }
        if (!QUALITY_READY.equals(message.qualityStatus())) {
            return "배치 품질 상태 " + message.qualityStatus();
        }
        boolean noBaselines = message.routineBaselines() == null
                || message.routineBaselines().isEmpty();
        boolean noStatistics = message.statistics() == null
                || message.statistics().isEmpty();
        if (noBaselines && noStatistics) {
            return "기준선과 통계가 모두 비어 있음";
        }
        return null;
    }

    /** 반영 여부를 판정하기 전에 꼭 있어야 하는 값들. */
    private boolean isProcessable(HouseholdProfileMessage message) {
        if (message == null
                || isBlank(message.householdId())
                || isBlank(message.profileVersion())
                || message.profileRevision() == null
                || message.profileRevision() < 1
                || (!DELIVERY_ACTIVE.equals(message.deliveryMode())
                    && !DELIVERY_SHADOW.equals(message.deliveryMode()))
                || message.asOfDate() == null
                || message.effectiveFrom() == null) {
            log.warn("필수 항목이 빠진 프로필 무시: householdId={}, profileVersion={}",
                    message == null ? null : message.householdId(),
                    message == null ? null : message.profileVersion());
            return false;
        }
        return true;
    }

    private int compareOrder(HouseholdProfileMessage incoming, HouseholdProfile current) {
        int byDate = incoming.asOfDate().compareTo(current.getAsOfDate());
        return byDate != 0
                ? byDate
                : Long.compare(incoming.profileRevision(), current.getProfileRevision());
    }

    private Comparator<HouseholdProfile> profileOrder() {
        return Comparator.comparing(HouseholdProfile::getAsOfDate)
                .thenComparingLong(HouseholdProfile::getProfileRevision)
                .thenComparing(HouseholdProfile::getId);
    }

    private List<HouseholdProfile> operationalCandidates(String householdId) {
        List<HouseholdProfile> result = new ArrayList<>();
        result.addAll(profiles.findByHouseholdIdAndStatusOrderByEffectiveFromDescIdDesc(
                householdId, HouseholdProfile.Status.ACTIVE));
        result.addAll(profiles.findByHouseholdIdAndStatusOrderByEffectiveFromDescIdDesc(
                householdId, HouseholdProfile.Status.PENDING));
        return result;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static String blankToUnknown(String value) {
        return isBlank(value) ? QUALITY_UNKNOWN : value;
    }
}

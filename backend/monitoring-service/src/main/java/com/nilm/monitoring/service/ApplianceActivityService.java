package com.nilm.monitoring.service;

import com.nilm.monitoring.config.RiskProperties;
import com.nilm.monitoring.config.enums.StateChangeTrigger;
import com.nilm.monitoring.domain.ApplianceState;
import com.nilm.monitoring.domain.ApplianceUsageEpisode;
import com.nilm.monitoring.domain.HouseholdDailyApplianceUsage;
import com.nilm.monitoring.domain.HouseholdDailyApplianceUsageId;
import com.nilm.monitoring.domain.HouseholdObservation;
import com.nilm.monitoring.domain.Subject;
import com.nilm.monitoring.domain.ValidUseContract;
import com.nilm.monitoring.dto.kafka.AnalysisSnapshotMessage;
import com.nilm.monitoring.repository.ApplianceStateRepository;
import com.nilm.monitoring.repository.ApplianceUsageEpisodeRepository;
import com.nilm.monitoring.repository.HouseholdDailyApplianceUsageRepository;
import com.nilm.monitoring.repository.HouseholdObservationRepository;
import com.nilm.monitoring.repository.SubjectRepository;
import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * analysis.snapshot.v1의 가전 ON/OFF 상태를 직전 상태와 비교해 전환을 가려낸다.
 *
 * <p>스냅샷은 이상 징후가 없어도 계속 들어온다. 그래서 이 경로가 위험 평가의
 * "현재 값" 공급처다. 마지막 관측 시각과 커버리지, 가전별 논리 사용, 마지막 활동 시각이
 * 모두 여기서 채워진다.
 *
 * <p>전환을 그대로 세지 않는다. Gold는 가전별 병합 간격 안의 ON 구간들을 한 번의 사용으로
 * 묶고 실제 사용시간 합계가 10초 이상일 때만 센다({@link ValidUseContract}).
 * 모니터링의 현재 값이 같은 이름으로 다른 것을 세면 프로필과의 비교가 성립하지 않으므로,
 * 전환을 {@link ApplianceUsageEpisode}로 묶어 같은 정의를 쓴다.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class ApplianceActivityService {

    /** 영업일 경계. 프로필의 일별 집계와 같은 시간대를 쓴다. */
    private static final ZoneId ZONE = ZoneId.of("Asia/Seoul");

    private final SubjectRepository subjects;
    private final ApplianceStateRepository applianceStates;
    private final HouseholdObservationRepository observations;
    private final ApplianceUsageEpisodeRepository episodes;
    private final HouseholdDailyApplianceUsageRepository dailyUsage;
    private final RiskAssessmentService assessments;
    private final RiskProperties riskProperties;
    private final PowerUsageAccumulator powerUsage;
    private final ApplicationEventPublisher publisher;

    @Transactional
    public void handle(AnalysisSnapshotMessage message) {
        if (!isProcessable(message)) {
            return;
        }

        // 스냅샷 토픽에는 이 서비스가 모르는 가구도 흘러온다.
        // 컨슈머를 멈추지 않고 조용히 건너뛴다.
        if (!subjects.existsByHouseholdId(message.householdId())) {
            log.debug("등록되지 않은 가구의 스냅샷 무시: householdId={}", message.householdId());
            return;
        }

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

        // 관측 신선도와 커버리지를 먼저 남긴다. 전환이 없어도 "보고 있다"는 사실은 갱신돼야
        // 평가가 조용한 시간을 관측 두절로 오해하지 않는다.
        // 가구 행 락을 잡지 않는다. 상시로 들어오는 흐름이 이벤트 처리와 부딪히면 안 된다.
        recordObservation(message, now);

        // 전환이 없어도 전력은 계속 쓰인다. 아래 조기 반환보다 앞에서 적산해야
        // 조용한 시간대의 그래프가 비지 않는다.
        powerUsage.accumulate(message, now);

        List<ApplianceState> stored =
                applianceStates.findByHouseholdId(message.householdId());
        if (isStale(stored, message.observedAt())) {
            log.debug("늦게 도착한 스냅샷 무시: householdId={}, observedAt={}",
                    message.householdId(), message.observedAt());
            return;
        }

        Map<String, ApplianceState> byType = new HashMap<>();
        for (ApplianceState state : stored) {
            byType.put(state.getApplianceType(), state);
        }

        List<String> turnedOff = new ArrayList<>();
        List<String> turnedOn = new ArrayList<>();
        List<String> seededOn = new ArrayList<>();
        Set<String> reportedOn = new HashSet<>();
        List<ApplianceState> created = new ArrayList<>();
        for (AnalysisSnapshotMessage.Appliance appliance : message.appliances()) {
            if (appliance == null || appliance.isOn() == null
                    || appliance.applianceType() == null
                    || appliance.applianceType().isBlank()) {
                continue;
            }
            if (appliance.isOn()) {
                reportedOn.add(appliance.applianceType());
            }
            ApplianceState previous = byType.get(appliance.applianceType());
            if (previous == null) {
                // 첫 스냅샷은 비교 대상이 없다. 전환으로 보지 않고 상태만 심어 둔다.
                created.add(new ApplianceState(
                        message.householdId(),
                        appliance.applianceType(),
                        appliance.isOn(),
                        message.observedAt()
                ));
                if (appliance.isOn()) {
                    seededOn.add(appliance.applianceType());
                }
                continue;
            }
            switch (previous.changeTo(appliance.isOn(), message.observedAt())) {
                case TURNED_ON -> turnedOn.add(appliance.applianceType());
                case TURNED_OFF -> turnedOff.add(appliance.applianceType());
                case NONE -> {
                    // 직전과 같은 상태다. 남길 것이 없다.
                }
            }
        }
        if (!created.isEmpty()) {
            applianceStates.saveAll(created);
        }

        for (String appliance : turnedOff) {
            closeEpisode(message.householdId(), appliance, message.observedAt(), now);
        }
        for (String appliance : turnedOn) {
            startEpisode(message.householdId(), appliance, message.observedAt(), false, now);
        }
        for (String appliance : seededOn) {
            // 처음 본 순간 이미 켜져 있었다. 언제 켜졌는지는 모른다.
            // 시작을 지어내지 않고 "시작을 보지 못한 사용"으로 연다.
            startEpisode(message.householdId(), appliance, message.observedAt(), true, now);
        }

        // 켜져 있는 동안에도 관측이 쌓이면 사용시간이 최소 기준을 넘는다.
        // 끝나기 전에도 그 시점에 유효로 확정한다.
        confirmOngoing(message.householdId(), reportedOn, message.observedAt(), now);

        if (turnedOff.isEmpty() && turnedOn.isEmpty()) {
            // 전환이 없으면 평가 입력도 그대로다. 매 스냅샷마다 평가하지 않는다.
            return;
        }

        // 분석 이벤트 소비와 같은 행을 갱신한다. 같은 락을 잡아 덮어쓰기를 막는다.
        List<Subject> matches = subjects.findHouseholdForUpdate(message.householdId());
        if (matches.size() != 1) {
            log.warn("가구에 대상자가 정확히 한 명이 아니어서 마지막 활동을 건너뛴다: householdId={}, 수={}",
                    message.householdId(), matches.size());
            return;
        }
        Subject subject = matches.get(0);

        if (!turnedOff.isEmpty()) {
            // 한 스냅샷에서 여러 대가 동시에 꺼지면 시각이 모두 같다.
            // 무엇을 골라도 등가이므로 코드 순으로 정해 결과를 재현 가능하게 둔다.
            String appliance = turnedOff.stream().min(Comparator.naturalOrder()).orElseThrow();
            if (subject.recordActivity(appliance, message.observedAt(), now)) {
                publisher.publishEvent(
                        new SubjectStateChanged(subject.getId(), StateChangeTrigger.ACTIVITY));
            }
            // 장시간 사용으로 세워 둔 이벤트 등급은 그 가전이 꺼지면 근거를 잃는다.
            if (assessments.releaseEventRiskFor(subject, turnedOff, now)) {
                // 슬롯이 비면 유효 등급이 내려간다. 담당자 화면이 알아야 한다.
                publisher.publishEvent(
                        new SubjectStateChanged(subject.getId(), StateChangeTrigger.ACTIVITY));
            }
        }

        // 상태가 바뀐 그 트랜잭션 안에서 평가한다.
        // 스냅샷이 롤백되면 그 상태로 낸 평가도 함께 사라져야 한다.
        assessments.evaluate(message.householdId(), now, StateChangeTrigger.ACTIVITY);
    }

    /**
     * 마지막 관측 시각과 커버리지를 갱신한다.
     *
     * <p>같은 가구의 스냅샷은 한 파티션으로 순서대로 들어오므로 읽고 쓰는 사이에
     * 끼어들 경쟁자가 없다. 드물게 겹치더라도 관측 시각이 되돌아가지 않는 것만 지키면 된다.
     */
    private void recordObservation(AnalysisSnapshotMessage message, OffsetDateTime now) {
        Duration gapThreshold = riskProperties.getObservationGapThreshold();
        observations.findById(message.householdId())
                .ifPresentOrElse(
                        observation -> observation.observe(
                                message.observedAt(), message.publishedAt(), now,
                                ZONE, gapThreshold),
                        () -> observations.save(new HouseholdObservation(
                                message.householdId(),
                                message.observedAt(),
                                message.publishedAt(),
                                now
                        ))
                );
    }

    /**
     * 사용을 연다. 직전 사용이 병합 간격 안에 끝났으면 새 사용이 아니라 같은 사용의
     * 다음 구간이다. 가까운 재시작을 두 번으로 세지 않기 위해서다.
     */
    private void startEpisode(
            String householdId,
            String applianceType,
            OffsetDateTime at,
            boolean startImputed,
            OffsetDateTime now
    ) {
        List<ApplianceUsageEpisode> open = episodes
                .findByHouseholdIdAndApplianceTypeAndEndedAtIsNull(householdId, applianceType);
        if (!open.isEmpty()) {
            // 이미 켜진 것으로 알고 있다. 중복 ON은 새 사용을 만들지 않는다.
            return;
        }

        List<ApplianceUsageEpisode> latest = episodes
                .findByHouseholdIdAndApplianceTypeAndEndedAtIsNotNullOrderByEndedAtDesc(
                        householdId, applianceType, Limit.of(1));
        if (!latest.isEmpty()) {
            ApplianceUsageEpisode previous = latest.get(0);
            long gap = Duration.between(previous.getEndedAt(), at).getSeconds();
            if (gap >= 0 && gap <= ValidUseContract.mergeGap(applianceType).getSeconds()) {
                previous.resume(at, now);
                return;
            }
        }

        if (episodes.findByHouseholdIdAndApplianceTypeAndStartedAt(householdId, applianceType, at)
                .isPresent()) {
            // 같은 시작 시각의 사용이 이미 있다. 재전송된 스냅샷이다.
            return;
        }
        episodes.save(new ApplianceUsageEpisode(
                householdId, applianceType, at, startImputed, ZONE, now));
    }

    /** 사용을 닫는다. 시작을 보지 못한 OFF로 없는 사용을 만들지 않는다. */
    private void closeEpisode(
            String householdId,
            String applianceType,
            OffsetDateTime at,
            OffsetDateTime now
    ) {
        List<ApplianceUsageEpisode> open = episodes
                .findByHouseholdIdAndApplianceTypeAndEndedAtIsNull(householdId, applianceType);
        if (open.isEmpty()) {
            return;
        }
        ApplianceUsageEpisode episode = open.get(0);
        LocalDate before = businessDate(episode.getObservedUntil());
        boolean confirmed = episode.close(at, now);
        projectIfNeeded(episode, before, at, confirmed);
    }

    /** 켜져 있는 사용에 관측을 하나 더한다. 종료 전에 유효가 확정될 수 있다. */
    private void confirmOngoing(
            String householdId,
            Set<String> reportedOn,
            OffsetDateTime at,
            OffsetDateTime now
    ) {
        if (reportedOn.isEmpty()) {
            return;
        }
        for (ApplianceUsageEpisode episode : episodes.findByHouseholdIdAndEndedAtIsNull(
                householdId)) {
            if (!reportedOn.contains(episode.getApplianceType())) {
                continue;
            }
            LocalDate before = businessDate(episode.getObservedUntil());
            boolean confirmed = episode.observe(at, now);
            projectIfNeeded(episode, before, at, confirmed);
        }
    }

    /**
     * 일별 사용 사실 투영을 다시 계산할지 정한다.
     *
     * <p>유효가 새로 확정된 순간과, 이어지던 사용이 자정을 넘긴 첫 순간에만 다시 센다.
     * 자정을 넘긴 사용은 새 날의 시작으로 세지 않지만 그 날 "썼다"는 사실은 남겨야 한다.
     */
    private void projectIfNeeded(
            ApplianceUsageEpisode episode,
            LocalDate observedBefore,
            OffsetDateTime at,
            boolean confirmed
    ) {
        LocalDate observedNow = businessDate(at);
        boolean crossedMidnight = episode.isValid() && !observedNow.equals(observedBefore);
        if (!confirmed && !crossedMidnight) {
            return;
        }
        Set<LocalDate> dates = new LinkedHashSet<>();
        dates.add(episode.getBusinessDate());
        dates.add(observedNow);
        for (LocalDate date : dates) {
            recomputeDailyUsage(episode.getHouseholdId(), episode.getApplianceType(), date);
        }
    }

    /**
     * 하루치 사용 사실을 에피소드에서 다시 계산한다.
     *
     * <p>증분으로 더하지 않는다. 같은 스냅샷이 두 번 처리돼도 같은 값이 나와야 한다.
     */
    private void recomputeDailyUsage(
            String householdId,
            String applianceType,
            LocalDate date
    ) {
        OffsetDateTime dayStart = date.atStartOfDay(ZONE).toOffsetDateTime();
        OffsetDateTime dayEnd = dayStart.plusDays(1);
        List<ApplianceUsageEpisode> overlapping =
                episodes.findValidOverlapping(householdId, applianceType, dayStart, dayEnd);
        if (overlapping.isEmpty()) {
            return;
        }

        OffsetDateTime first = null;
        OffsetDateTime last = null;
        int starts = 0;
        for (ApplianceUsageEpisode episode : overlapping) {
            // 자정을 넘겨 이어진 사용의 그 날 첫 시각은 자정이다. 배치와 같은 규칙이다.
            OffsetDateTime startedAt = episode.getStartedAt().isBefore(dayStart)
                    ? dayStart
                    : episode.getStartedAt();
            if (first == null || startedAt.isBefore(first)) {
                first = startedAt;
            }
            if (last == null || startedAt.isAfter(last)) {
                last = startedAt;
            }
            if (date.equals(episode.getBusinessDate()) && !episode.isStartImputed()) {
                starts++;
            }
        }

        OffsetDateTime firstOnAt = first;
        OffsetDateTime lastOnAt = last;
        int startCount = starts;
        dailyUsage
                .findById(new HouseholdDailyApplianceUsageId(householdId, applianceType, date))
                .ifPresentOrElse(
                        usage -> usage.apply(firstOnAt, lastOnAt, startCount),
                        () -> dailyUsage.save(new HouseholdDailyApplianceUsage(
                                householdId, applianceType, date,
                                firstOnAt, lastOnAt, startCount))
                );
    }

    private LocalDate businessDate(OffsetDateTime at) {
        return at.atZoneSameInstant(ZONE).toLocalDate();
    }

    private boolean isProcessable(AnalysisSnapshotMessage message) {
        if (message == null || message.householdId() == null
                || message.householdId().isBlank()
                || message.observedAt() == null
                || message.appliances() == null
                || message.appliances().isEmpty()) {
            log.warn("가전 상태를 읽을 수 없는 스냅샷 무시: snapshotId={}",
                    message == null ? null : message.snapshotId());
            return false;
        }
        return true;
    }

    /**
     * 마지막으로 반영한 전환보다 앞선 스냅샷이면 건너뛴다.
     * 파티션 재배치나 재전송으로 과거 상태가 뒤늦게 덮어쓰는 일을 막는다.
     */
    private boolean isStale(List<ApplianceState> stored, OffsetDateTime observedAt) {
        return stored.stream()
                .map(ApplianceState::getChangedAt)
                .max(Comparator.naturalOrder())
                .filter(observedAt::isBefore)
                .isPresent();
    }
}

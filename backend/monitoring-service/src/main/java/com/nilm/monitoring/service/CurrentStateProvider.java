package com.nilm.monitoring.service;

import com.nilm.monitoring.domain.ApplianceUsageEpisode;
import com.nilm.monitoring.domain.HouseholdObservation;
import com.nilm.monitoring.repository.ApplianceUsageEpisodeRepository;
import com.nilm.monitoring.repository.HouseholdObservationRepository;
import com.nilm.monitoring.risk.ActivityLedger;
import com.nilm.monitoring.risk.CurrentState;
import com.nilm.monitoring.risk.EvaluationPoint;
import com.nilm.monitoring.risk.ObservationQuality;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 평가 한 번이 볼 "지금"을 한 번에 읽어 값으로 고정한다.
 *
 * <p>{@link RiskAssessmentService}의 유일한 입력 경로다. 옛 경로는 마지막 관측 시각과
 * 전환 횟수만 실어서, 관측 공백을 증명할 수도 없고 Gold의 유효 사용 정의와 같은 숫자를
 * 만들 수도 없었다. 여기서는 두 가지를 모두 싣는다.
 *
 * <ul>
 *   <li><b>관측 품질</b>: 마지막 관측 시각(신선도), 끊김 없는 관측의 시작, 영업일별 실제
 *       관측 초. 평가 기준 시각이 전날 구간을 가리킬 수 있어 하루 전까지 함께 싣는다.</li>
 *   <li><b>사용 원장</b>: 병합과 최소 사용시간을 통과한 유효 사용들. 무활동의 기준점이 될
 *       마지막 사용은 조회 구간 밖이어도 반드시 한 건 싣는다.</li>
 * </ul>
 *
 * <p>두 가지를 모두 실어야 활동 감소(A)가 계산된다. 옛 입력 계약으로는 A가 제외됐다.
 * v1 점수식의 루틴 미사용(M)·무활동(I)은 분석 서비스로 넘어갔다.
 */
@Service
@RequiredArgsConstructor
public class CurrentStateProvider {

    /** 영업일 경계. 프로필을 만든 배치와 같은 시간대를 쓴다. */
    private static final ZoneId ZONE = ZoneId.of("Asia/Seoul");

    private final HouseholdObservationRepository observations;
    private final ApplianceUsageEpisodeRepository episodes;

    @Transactional(readOnly = true)
    public CurrentState of(String householdId, boolean away, OffsetDateTime now) {
        EvaluationPoint point = EvaluationPoint.of(now, ZONE);
        LocalDate today = now.atZoneSameInstant(ZONE).toLocalDate();
        OffsetDateTime todayStart = today.atStartOfDay(ZONE).toOffsetDateTime();

        Optional<HouseholdObservation> observation = observations.findById(householdId);
        ObservationQuality quality = observation
                .map(row -> quality(row, today, point.businessDate()))
                .orElseGet(() -> ObservationQuality.untracked(null));

        // 누적 활동량은 평가 기준 시각이 속한 영업일에서, 루틴 미사용은 오늘에서 읽는다.
        // 자정 직후에는 둘이 다른 날이라 이른 쪽부터 담는다.
        OffsetDateTime from = point.businessDayStart().isBefore(todayStart)
                ? point.businessDayStart()
                : todayStart;
        return new CurrentState(now, away, quality, ledger(householdId, from, point.at()));
    }

    private ObservationQuality quality(
            HouseholdObservation observation,
            LocalDate today,
            LocalDate businessDate
    ) {
        Map<LocalDate, Long> covered = new LinkedHashMap<>();
        put(covered, observation, today);
        put(covered, observation, businessDate);
        return new ObservationQuality(
                observation.getLastObservedAt(),
                observation.coverageTracked(),
                observation.getContinuousSince(),
                covered);
    }

    private void put(
            Map<LocalDate, Long> covered,
            HouseholdObservation observation,
            LocalDate date
    ) {
        OptionalLong seconds = observation.coveredSecondsOn(date);
        if (seconds.isPresent()) {
            covered.put(date, seconds.getAsLong());
        }
    }

    /**
     * 유효 사용 원장.
     *
     * <p>조회 구간에 걸친 사용에 더해, 기준 시각 이전에 끝난 마지막 사용을 한 건 더 담는다.
     * 며칠째 활동이 없는 가구의 무활동 경과는 그 한 건이 없으면 잴 수 없다.
     */
    private ActivityLedger ledger(String householdId, OffsetDateTime from, OffsetDateTime at) {
        List<ApplianceUsageEpisode> rows =
                new ArrayList<>(episodes.findValidSince(householdId, from));
        List<ApplianceUsageEpisode> lastEnded =
                episodes.findLatestValidEndedAtOrBefore(householdId, at, Limit.of(1));
        for (ApplianceUsageEpisode episode : lastEnded) {
            if (rows.stream().noneMatch(row -> row.getId().equals(episode.getId()))) {
                rows.add(episode);
            }
        }

        List<ActivityLedger.Use> uses = rows.stream()
                .map(row -> new ActivityLedger.Use(
                        row.getApplianceType(),
                        row.getStartedAt(),
                        row.getEndedAt(),
                        row.isStartImputed(),
                        row.getBusinessDate()))
                .toList();
        return ActivityLedger.of(uses);
    }
}

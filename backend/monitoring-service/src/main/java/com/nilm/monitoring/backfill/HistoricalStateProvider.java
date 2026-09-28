package com.nilm.monitoring.backfill;

import com.nilm.monitoring.domain.ApplianceUsageEpisode;
import com.nilm.monitoring.domain.HouseholdObservation;
import com.nilm.monitoring.domain.ValidUseContract;
import com.nilm.monitoring.dto.kafka.AnalysisSnapshotMessage;
import com.nilm.monitoring.risk.*;
import java.time.*;
import java.util.*;

/** Event-time reconstruction; deliberately has no repositories or notification services. */
public final class HistoricalStateProvider {
    static final ZoneId ZONE = ZoneId.of("Asia/Seoul");
    private final String household;
    private final Duration gapThreshold;
    private HouseholdObservation observation;
    private AnalysisSnapshotMessage last;
    private final Map<String, Boolean> states = new HashMap<>();
    private final Map<String, ApplianceUsageEpisode> latest = new HashMap<>();
    private final List<ApplianceUsageEpisode> episodes = new ArrayList<>();

    public HistoricalStateProvider(String household, Duration gapThreshold) {
        this.household = Objects.requireNonNull(household);
        this.gapThreshold = Objects.requireNonNull(gapThreshold);
        if (household.isBlank() || gapThreshold.isNegative()) throw new IllegalArgumentException("invalid state configuration");
    }

    public void accept(AnalysisSnapshotMessage snapshot) {
        if (!household.equals(snapshot.householdId()) || snapshot.observedAt() == null
                || snapshot.appliances() == null || !Integer.valueOf(1).equals(snapshot.schemaVersion())) {
            throw new IllegalArgumentException("invalid snapshot or household mismatch");
        }
        Set<String> seen = new HashSet<>();
        for (var appliance : snapshot.appliances()) {
            if (appliance == null || appliance.applianceType() == null || appliance.applianceType().isBlank()
                    || appliance.isOn() == null || !seen.add(appliance.applianceType())) {
                throw new IllegalArgumentException("invalid/duplicate appliance");
            }
        }
        if (last != null && !snapshot.observedAt().isAfter(last.observedAt())) {
            if (snapshot.equals(last)) return; // exact adjacent retry
            throw new IllegalArgumentException("snapshots must be strictly ordered; conflicting timestamp");
        }
        var at = snapshot.observedAt();
        if (observation == null) observation = new HouseholdObservation(household, at, at, at);
        else observation.observe(at, at, at, ZONE, gapThreshold);
        for (var appliance : snapshot.appliances()) {
            String type = appliance.applianceType();
            Boolean previous = states.put(type, appliance.isOn());
            var episode = latest.get(type);
            if (appliance.isOn()) {
                if (previous == null || !previous) {
                    boolean merge = episode != null && episode.getEndedAt() != null
                            && Duration.between(episode.getEndedAt(), at).compareTo(ValidUseContract.mergeGap(type)) <= 0;
                    if (merge) episode.resume(at, at);
                    else {
                        episode = new ApplianceUsageEpisode(household, type, at, previous == null, ZONE, at);
                        latest.put(type, episode);
                        episodes.add(episode);
                    }
                }
                episode.observe(at, at);
            } else if (Boolean.TRUE.equals(previous)) episode.close(at, at);
        }
        last = snapshot;
    }

    public CurrentState at(OffsetDateTime at, boolean away) {
        if (last != null && last.observedAt().isAfter(at)) throw new IllegalArgumentException("future snapshot in state");
        ObservationQuality quality = ObservationQuality.untracked(null);
        if (observation != null) {
            Map<LocalDate, Long> coverage = new HashMap<>();
            LocalDate today = at.atZoneSameInstant(ZONE).toLocalDate();
            for (var date : Set.of(today, today.minusDays(1))) {
                observation.coveredSecondsOn(date).ifPresent(value -> coverage.put(date, value));
            }
            quality = new ObservationQuality(observation.getLastObservedAt(), observation.coverageTracked(),
                    observation.getContinuousSince(), coverage);
        }
        var uses = episodes.stream().filter(ApplianceUsageEpisode::isValid)
                .map(e -> new ActivityLedger.Use(e.getApplianceType(), e.getStartedAt(), e.getEndedAt(),
                        e.isStartImputed(), e.getBusinessDate())).toList();
        return new CurrentState(at, away, quality, ActivityLedger.of(uses));
    }
}

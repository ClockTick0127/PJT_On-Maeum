package com.nilm.monitoring.backfill;

import com.nilm.monitoring.dto.kafka.HouseholdProfileMessage;
import com.nilm.monitoring.service.ResolvedProfile;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;

/** Selects archived Gold messages without changing ACTIVE database rows. */
public final class HistoricalProfileResolver {
    static final Set<Integer> SUPPORTED_SCHEMA_VERSIONS = Set.of(1, 2);
    private final List<HouseholdProfileMessage> profiles;
    public HistoricalProfileResolver(List<HouseholdProfileMessage> profiles) {
        this.profiles = List.copyOf(profiles);
        Set<String> versions = new HashSet<>();
        for (var p : profiles) {
            // gold.household-profile.v1 carries schema_version 2 since delivery_mode was added;
            // the live consumer ignores the field, so accept both published versions here.
            if (!SUPPORTED_SCHEMA_VERSIONS.contains(p.schemaVersion()) || p.householdId() == null || p.profileVersion() == null
                    || p.asOfDate() == null || p.effectiveFrom() == null || p.publishedAt() == null
                    || p.profileRevision() == null || p.routineBaselines() == null || p.statistics() == null
                    || !versions.add(p.householdId() + ":" + p.profileVersion())) {
                throw new IllegalArgumentException("invalid or duplicate archived profile");
            }
        }
    }

    public Optional<ResolvedProfile> resolve(String household, OffsetDateTime at, Duration maxAge) {
        LocalDate day = at.atZoneSameInstant(HistoricalStateProvider.ZONE).toLocalDate();
        return profiles.stream().filter(p -> household.equals(p.householdId()))
                .filter(p -> "ACTIVE".equals(p.deliveryMode()) && "READY".equals(p.qualityStatus()))
                .filter(p -> p.asOfDate().isBefore(day) && !p.effectiveFrom().isAfter(at) && !p.publishedAt().isAfter(at))
                .max(Comparator.comparing(HouseholdProfileMessage::asOfDate)
                        .thenComparing(HouseholdProfileMessage::profileRevision))
                .map(p -> {
                    var baselines = p.routineBaselines().stream().map(b -> new ResolvedProfile.RoutineBaseline(
                            b.applianceType(), b.baselineScope(), b.weekday(), b.sampleDays(), b.activeDays(),
                            b.dailyUseProbability(), b.reliabilityWeight(), b.firstUseTimeP50Second(),
                            b.expectedUntilSecond(), b.preferredWindowStartSecond(), b.preferredWindowEndSecond(),
                            b.qualityStatus(), Boolean.TRUE.equals(b.enabled()))).toList();
                    Map<String, ResolvedProfile.Statistic> statistics = new HashMap<>();
                    for (var s : p.statistics()) {
                        String key = ResolvedProfile.statisticKey(s.metricName(), s.applianceType(), s.weekdayGroup(), s.timeBucket());
                        if (statistics.put(key, new ResolvedProfile.Statistic(s.metricName(), s.applianceType(),
                                s.weekdayGroup(), s.timeBucket(), s.sampleCount(), s.eligibleDayCount(), s.p50(),
                                s.p90(), s.mad(), s.unit(), s.qualityStatus())) != null) {
                            throw new IllegalArgumentException("duplicate statistic " + key);
                        }
                    }
                    return new ResolvedProfile(p.profileVersion(), p.asOfDate(), p.effectiveFrom(),
                            ChronoUnit.DAYS.between(p.asOfDate(), day) > maxAge.toDays(), baselines, statistics);
                });
    }
}

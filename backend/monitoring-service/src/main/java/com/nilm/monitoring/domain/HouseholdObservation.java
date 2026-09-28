package com.nilm.monitoring.domain;

import jakarta.persistence.*;
import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Optional;
import java.util.OptionalLong;
import lombok.Getter;

/**
 * 가구별 관측 신선도와 관측 커버리지.
 *
 * <p>평가가 "지금 값"을 믿어도 되는지 판정하는 근거다. 스냅샷이 끊긴 동안의 무활동은
 * 대상자가 가만히 있었다는 뜻이 아니라 우리가 보지 못했다는 뜻이므로,
 * 이 값이 오래되면 현재 값을 쓰는 지표를 계산하지 않는다.
 *
 * <p>마지막 관측 시각만으로는 부족하다. 공백 뒤에 스냅샷 한 건이 도착하면 신선도는
 * 곧바로 회복되지만, 그 한 건은 공백 동안 무슨 일이 있었는지 아무것도 말해 주지 않는다.
 * "오늘 한 번도 쓰지 않았다" 같은 하루치 단정을 그 위에 세우지 않도록 영업일별로 실제
 * 관측한 초를 함께 적고, 끊김 없이 이어진 관측의 시작 시각을 들고 있는다.
 *
 * <p>커버리지 값이 비어 있는 행은 "0초를 관측했다"가 아니라 "커버리지를 모른다"는 뜻이다.
 * 이 스키마 이전에 만들어진 행이 그렇다. 모르는 구간을 정상 관측으로 바꾸지 않는다.
 *
 * <p>대상자 행과 분리해 둔다. 스냅샷은 이상 징후가 없어도 계속 들어오는데
 * 그때마다 가구 행 락을 잡으면 이벤트 처리와 불필요하게 부딪힌다.
 */
@Entity
@Getter
@Table(name = "household_observations")
public class HouseholdObservation {

    @Id
    @Column(name = "household_id")
    private String householdId;

    @Column(name = "last_observed_at", nullable = false)
    private OffsetDateTime lastObservedAt;

    @Column(name = "last_published_at")
    private OffsetDateTime lastPublishedAt;

    /** 끊김 없이 이어진 관측이 시작된 시각. 공백이 생길 때마다 그 뒤로 옮긴다. */
    @Column(name = "continuous_since")
    private OffsetDateTime continuousSince;

    @Column(name = "coverage_date")
    private LocalDate coverageDate;

    @Column(name = "covered_seconds", nullable = false)
    private long coveredSeconds;

    @Column(name = "previous_coverage_date")
    private LocalDate previousCoverageDate;

    @Column(name = "previous_covered_seconds", nullable = false)
    private long previousCoveredSeconds;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    protected HouseholdObservation() {
    }

    public HouseholdObservation(
            String householdId,
            OffsetDateTime lastObservedAt,
            OffsetDateTime lastPublishedAt,
            OffsetDateTime updatedAt
    ) {
        this.householdId = householdId;
        this.lastObservedAt = lastObservedAt;
        this.lastPublishedAt = lastPublishedAt;
        this.updatedAt = updatedAt;
        // 첫 관측 이전은 보지 않았다. 커버리지도 여기서부터 쌓는다.
        this.continuousSince = lastObservedAt;
    }

    /**
     * 더 새로운 관측만 반영한다.
     * 파티션 재배치나 재전송으로 과거 스냅샷이 뒤늦게 도착해도 신선도가 되돌아가지 않는다.
     *
     * <p>직전 관측과의 간격이 공백 기준 이내면 그 사이를 "본 구간"으로 적는다.
     * 기준을 넘으면 아무것도 적지 않고 끊김 없는 관측의 시작만 이번 관측으로 옮긴다.
     *
     * @return 값이 실제로 갱신되었으면 true
     */
    public boolean observe(
            OffsetDateTime observedAt,
            OffsetDateTime publishedAt,
            OffsetDateTime now,
            ZoneId zone,
            Duration gapThreshold
    ) {
        if (observedAt == null || observedAt.isBefore(lastObservedAt)) {
            return false;
        }
        long elapsed = Duration.between(lastObservedAt, observedAt).getSeconds();
        if (elapsed > gapThreshold.getSeconds()) {
            this.continuousSince = observedAt;
        } else {
            cover(lastObservedAt, observedAt, zone);
            if (continuousSince == null) {
                this.continuousSince = lastObservedAt;
            }
        }
        this.lastObservedAt = observedAt;
        this.lastPublishedAt = publishedAt;
        this.updatedAt = now;
        return true;
    }

    /** 영업일 하루에 실제로 관측한 초. 집계하지 않은 날이면 비어 있다. */
    public OptionalLong coveredSecondsOn(LocalDate date) {
        if (date.equals(coverageDate)) {
            return OptionalLong.of(coveredSeconds);
        }
        if (date.equals(previousCoverageDate)) {
            return OptionalLong.of(previousCoveredSeconds);
        }
        return OptionalLong.empty();
    }

    /** 커버리지를 집계하기 시작했는가. 이 스키마 이전의 행은 아직 아니다. */
    public boolean coverageTracked() {
        return coverageDate != null;
    }

    public Optional<OffsetDateTime> continuousSince() {
        return Optional.ofNullable(continuousSince);
    }

    /**
     * 본 구간 하나를 커버리지에 더한다. 영업일 경계를 넘으면 날짜별로 쪼개 담는다.
     * 공백 기준보다 짧은 구간만 들어오므로 하루를 통째로 건너뛰는 일은 없다.
     */
    private void cover(OffsetDateTime from, OffsetDateTime to, ZoneId zone) {
        LocalDate startDate = from.atZoneSameInstant(zone).toLocalDate();
        LocalDate endDate = to.atZoneSameInstant(zone).toLocalDate();
        if (startDate.equals(endDate)) {
            add(startDate, Duration.between(from, to).getSeconds());
            return;
        }
        OffsetDateTime boundary = endDate.atStartOfDay(zone).toOffsetDateTime();
        add(startDate, Duration.between(from, boundary).getSeconds());
        add(endDate, Duration.between(boundary, to).getSeconds());
    }

    private void add(LocalDate date, long seconds) {
        if (seconds <= 0) {
            return;
        }
        if (date.equals(coverageDate)) {
            this.coveredSeconds += seconds;
            return;
        }
        if (coverageDate == null) {
            this.coverageDate = date;
            this.coveredSeconds = seconds;
            return;
        }
        if (date.isAfter(coverageDate)) {
            this.previousCoverageDate = coverageDate;
            this.previousCoveredSeconds = coveredSeconds;
            this.coverageDate = date;
            this.coveredSeconds = seconds;
            return;
        }
        if (date.equals(previousCoverageDate)) {
            this.previousCoveredSeconds += seconds;
        }
        // 이보다 오래된 날짜는 평가가 보지 않는다. 버린다.
    }
}

package com.nilm.monitoring.domain;

import jakarta.persistence.*;
import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import lombok.Getter;

/**
 * 가전 하나의 논리 사용 한 건. 스냅샷의 ON/OFF 전환을 Gold와 같은 규칙으로 묶은 결과다.
 *
 * <p>OFF→ON 전환마다 한 번으로 세면 3초짜리 잡음도, 30초 뒤의 재시작도 각각 한 번이 된다.
 * Gold는 병합 간격 안의 ON 구간들을 하나로 묶고 실제 사용시간 합계가
 * {@link ValidUseContract#MINIMUM_ACTIVE} 이상일 때만 센다. 두 숫자가 같은 뜻을 가지려면
 * 모니터링도 같은 단위로 세야 한다.
 *
 * <p>진행 중과 확정을 나눈다. 켜져 있는 동안에도 관측이 쌓여 사용시간이 기준을 넘으면
 * 그 시점에 유효로 확정한다. 꺼지면 남은 구간까지 더해 다시 확인한다.
 * 사용시간은 줄어들지 않으므로 유효는 한 번 서면 되돌아가지 않는다.
 */
@Entity
@Getter
@Table(name = "appliance_usage_episodes")
public class ApplianceUsageEpisode {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "household_id", nullable = false)
    private String householdId;

    @Column(name = "appliance_type", nullable = false, length = 50)
    private String applianceType;

    @Column(name = "started_at", nullable = false)
    private OffsetDateTime startedAt;

    @Column(name = "start_imputed", nullable = false)
    private boolean startImputed;

    @Column(name = "ended_at")
    private OffsetDateTime endedAt;

    @Column(name = "segment_started_at")
    private OffsetDateTime segmentStartedAt;

    @Column(name = "observed_until")
    private OffsetDateTime observedUntil;

    @Column(name = "active_seconds", nullable = false)
    private long activeSeconds;

    @Column(name = "segment_count", nullable = false)
    private int segmentCount;

    @Column(name = "is_valid", nullable = false)
    private boolean valid;

    @Column(name = "business_date", nullable = false)
    private LocalDate businessDate;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    protected ApplianceUsageEpisode() {
    }

    /**
     * 새 사용을 연다.
     *
     * @param startImputed 첫 스냅샷이 이미 ON이어서 시작 시각을 보지 못한 경우.
     *                     시작 횟수로 세지 않는다
     */
    public ApplianceUsageEpisode(
            String householdId,
            String applianceType,
            OffsetDateTime startedAt,
            boolean startImputed,
            ZoneId zone,
            OffsetDateTime updatedAt
    ) {
        this.householdId = householdId;
        this.applianceType = applianceType;
        this.startedAt = startedAt;
        this.startImputed = startImputed;
        this.segmentStartedAt = startedAt;
        this.observedUntil = startedAt;
        this.activeSeconds = 0;
        this.segmentCount = 1;
        this.valid = false;
        this.businessDate = startedAt.atZoneSameInstant(zone).toLocalDate();
        this.updatedAt = updatedAt;
    }

    public boolean ongoing() {
        return segmentStartedAt != null;
    }

    /** 지금까지 확인된 실제 사용시간. 진행 중이면 마지막 관측까지만 센다. */
    public long observedActiveSeconds() {
        if (!ongoing()) {
            return activeSeconds;
        }
        return activeSeconds + Math.max(
                0, Duration.between(segmentStartedAt, observedUntil).getSeconds());
    }

    /**
     * 병합 간격 안에서 다시 켜졌다. 새 사용이 아니라 같은 사용의 다음 구간이다.
     *
     * @return 실제로 이어 붙였으면 true
     */
    public boolean resume(OffsetDateTime at, OffsetDateTime updatedAt) {
        if (ongoing() || endedAt == null || at.isBefore(endedAt)) {
            return false;
        }
        this.segmentStartedAt = at;
        this.observedUntil = at;
        this.endedAt = null;
        this.segmentCount++;
        this.updatedAt = updatedAt;
        return true;
    }

    /**
     * 켜져 있는 동안 관측이 하나 더 도착했다. 아직 끝나지 않았어도
     * 사용시간이 기준을 넘으면 그 자리에서 유효로 확정한다.
     *
     * @return 이번 관측으로 유효가 새로 확정됐으면 true
     */
    public boolean observe(OffsetDateTime at, OffsetDateTime updatedAt) {
        if (!ongoing() || at.isBefore(observedUntil)) {
            return false;
        }
        this.observedUntil = at;
        this.updatedAt = updatedAt;
        return confirm();
    }

    /**
     * 꺼졌다. 진행 중이던 구간을 사용시간에 더하고 유효성을 다시 본다.
     *
     * @return 이번 종료로 유효가 새로 확정됐으면 true
     */
    public boolean close(OffsetDateTime at, OffsetDateTime updatedAt) {
        if (!ongoing()) {
            return false;
        }
        OffsetDateTime end = at.isBefore(segmentStartedAt) ? segmentStartedAt : at;
        this.activeSeconds += Duration.between(segmentStartedAt, end).getSeconds();
        this.segmentStartedAt = null;
        this.observedUntil = end;
        this.endedAt = end;
        this.updatedAt = updatedAt;
        return confirm();
    }

    /** 기준 시각에 이 사용이 진행 중이었는가. */
    public boolean activeAt(OffsetDateTime at) {
        if (startedAt.isAfter(at)) {
            return false;
        }
        return endedAt == null || endedAt.isAfter(at);
    }

    private boolean confirm() {
        if (valid || !ValidUseContract.valid(observedActiveSeconds())) {
            return false;
        }
        this.valid = true;
        return true;
    }
}

package com.nilm.monitoring.domain;

import jakarta.persistence.*;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

@Entity
@Getter
@Table(name = "hourly_power_usage")
@IdClass(HourlyPowerUsageId.class)
public class HourlyPowerUsage {

    @Id
    @Column(name = "household_id", nullable = false)
    private String householdId;

    @Id
    @Column(name = "bucket_start_at", nullable = false)
    private OffsetDateTime bucketStartAt;

    @Column(
            name = "energy_wh",
            nullable = false,
            precision = 18,
            scale = 6
    )
    private BigDecimal energyWh; // 1시간동안 전력량

    /**
     * 이 구간에서 실제로 적산에 쓰인 시간(초). 최대 3600.
     * 전력량이 0이어도 이 값이 차 있으면 "보고 있었는데 안 썼다"는 뜻이고,
     * 0이면 그 시간을 보지 못했다는 뜻이다.
     */
    @Column(name = "observed_seconds", nullable = false)
    private int observedSeconds;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt; // 이 구간 집계가 마지막으로 반영된 시각

    protected HourlyPowerUsage() {
    }

    public HourlyPowerUsage(
            String householdId,
            OffsetDateTime bucketStartAt,
            BigDecimal energyWh,
            int observedSeconds,
            OffsetDateTime updatedAt
    ) {
        this.householdId = householdId;
        this.bucketStartAt = bucketStartAt;
        this.energyWh = energyWh;
        this.observedSeconds = observedSeconds;
        this.updatedAt = updatedAt;
    }

    /** 스냅샷 하나가 만든 전력량을 이 구간에 더한다. 구간은 여러 스냅샷에 걸쳐 채워진다. */
    public void add(BigDecimal amount, int seconds, OffsetDateTime now) {
        this.energyWh = this.energyWh.add(amount);
        this.observedSeconds += seconds;
        this.updatedAt = now;
    }
}

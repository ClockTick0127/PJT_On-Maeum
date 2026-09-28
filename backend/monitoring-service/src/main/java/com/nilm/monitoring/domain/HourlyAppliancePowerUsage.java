package com.nilm.monitoring.domain;

import jakarta.persistence.*;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

/**
 * 가전별로 분해된 시간 구간 전력량. 구간 기준은 {@link HourlyPowerUsage}와 같다.
 */
@Entity
@Getter
@Table(name = "hourly_appliance_power_usage")
@IdClass(HourlyAppliancePowerUsageId.class)
public class HourlyAppliancePowerUsage {

    @Id
    @Column(name = "household_id", nullable = false)
    private String householdId;

    @Id
    @Column(name = "bucket_start_at", nullable = false)
    private OffsetDateTime bucketStartAt;

    @Id
    @Column(name = "appliance_type", nullable = false, length = 50)
    private String applianceType;

    @Column(
            name = "energy_wh",
            nullable = false,
            precision = 18,
            scale = 6
    )
    private BigDecimal energyWh;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    protected HourlyAppliancePowerUsage() {
    }

    public HourlyAppliancePowerUsage(
            String householdId,
            OffsetDateTime bucketStartAt,
            String applianceType,
            BigDecimal energyWh,
            OffsetDateTime updatedAt
    ) {
        this.householdId = householdId;
        this.bucketStartAt = bucketStartAt;
        this.applianceType = applianceType;
        this.energyWh = energyWh;
        this.updatedAt = updatedAt;
    }

    /** 배분된 전력량을 이 구간에 더한다. 기준은 {@link HourlyPowerUsage#add}와 같다. */
    public void add(BigDecimal amount, OffsetDateTime now) {
        this.energyWh = this.energyWh.add(amount);
        this.updatedAt = now;
    }
}

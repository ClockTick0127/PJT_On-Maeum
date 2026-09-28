package com.nilm.device.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;

@Entity
@Table(name = "households")
public class Household {

    @Id
    @Column(name = "house_id", length = 10)
    private String houseId;

    @Column(nullable = false, length = 50)
    private String alias;

    @Column(name = "grace_minutes", nullable = false)
    private int graceMinutes = 120;

    @Column(name = "away_until")
    private OffsetDateTime awayUntil;

    @Column(name = "created_at", nullable = false, updatable = false, insertable = false)
    private OffsetDateTime createdAt;

    protected Household() {
    }

    public Household(String houseId, String alias, Integer graceMinutes) {
        this.houseId = houseId;
        this.alias = alias;
        if (graceMinutes != null) {
            this.graceMinutes = graceMinutes;
        }
    }

    public String getHouseId() {
        return houseId;
    }

    public String getAlias() {
        return alias;
    }

    public int getGraceMinutes() {
        return graceMinutes;
    }

    public OffsetDateTime getAwayUntil() {
        return awayUntil;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
}

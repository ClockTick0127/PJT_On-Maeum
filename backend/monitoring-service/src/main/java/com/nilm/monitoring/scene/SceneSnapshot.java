package com.nilm.monitoring.scene;

import jakarta.persistence.*;
import lombok.Getter;

/** Demo evidence is isolated from boolean appliance state and risk/notification tables. */
@Entity
@Getter
@Table(name = "selected_scene_snapshots", uniqueConstraints = @UniqueConstraint(
        columnNames = {"household_id", "run_id", "profile_id", "source_index"}))
public class SceneSnapshot {
    @Id @Column(length = 36) private String id;
    @Column(name = "household_id", nullable = false, length = 50) private String householdId;
    @Column(name = "run_id", nullable = false, length = 100) private String runId;
    @Column(name = "profile_id", nullable = false, length = 100) private String profileId;
    @Column(name = "source_index", nullable = false) private long sourceIndex;
    @Column(nullable = false, columnDefinition = "text") private String payload;

    protected SceneSnapshot() {}

    public SceneSnapshot(String id, String householdId, String runId, String profileId,
                         long sourceIndex, String payload) {
        this.id = id;
        this.householdId = householdId;
        this.runId = runId;
        this.profileId = profileId;
        this.sourceIndex = sourceIndex;
        this.payload = payload;
    }
}

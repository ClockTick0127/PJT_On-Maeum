package com.nilm.monitoring.scene;

import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SceneSnapshotRepository extends JpaRepository<SceneSnapshot, String> {
    Optional<SceneSnapshot> findTopByHouseholdIdAndRunIdAndProfileIdOrderBySourceIndexDesc(
            String householdId, String runId, String profileId);
    Optional<SceneSnapshot> findByHouseholdIdAndRunIdAndProfileIdAndSourceIndex(
            String householdId, String runId, String profileId, long sourceIndex);
}

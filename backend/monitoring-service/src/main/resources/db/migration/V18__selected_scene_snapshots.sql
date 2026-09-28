CREATE TABLE selected_scene_snapshots (
    id VARCHAR(36) PRIMARY KEY,
    household_id VARCHAR(50) NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    profile_id VARCHAR(100) NOT NULL,
    source_index BIGINT NOT NULL CHECK (source_index >= 0),
    payload TEXT NOT NULL,
    UNIQUE (household_id, run_id, profile_id, source_index)
);

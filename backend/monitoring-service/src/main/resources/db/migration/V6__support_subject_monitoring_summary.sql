ALTER TABLE subjects
    ADD COLUMN state_version BIGINT NOT NULL DEFAULT 1;

ALTER TABLE subjects
    ADD COLUMN current_risk_level VARCHAR(20) NOT NULL DEFAULT 'NORMAL';

ALTER TABLE subjects
    ADD COLUMN current_risk_score INTEGER NOT NULL DEFAULT 0;

ALTER TABLE subjects
    ADD COLUMN last_activity_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE subjects
    ADD COLUMN last_activity_appliance VARCHAR(100);

ALTER TABLE subjects
    ADD COLUMN updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE subjects
    ADD CONSTRAINT chk_subjects_current_risk_level
        CHECK (current_risk_level IN ('NORMAL', 'WARNING', 'DANGER'));

ALTER TABLE subjects
    ADD CONSTRAINT chk_subjects_current_risk_score
        CHECK (current_risk_score BETWEEN 0 AND 100);

ALTER TABLE notifications
    ADD COLUMN responded_at TIMESTAMP WITH TIME ZONE;

UPDATE subjects
SET current_risk_level = (
        SELECT e.risk_level
        FROM analysis_events e
        WHERE e.subject_id = subjects.id
        ORDER BY e.occurred_at DESC, e.id DESC
        LIMIT 1
    ),
    current_risk_score = (
        SELECT e.risk_score
        FROM analysis_events e
        WHERE e.subject_id = subjects.id
        ORDER BY e.occurred_at DESC, e.id DESC
        LIMIT 1
    )
WHERE EXISTS (
    SELECT 1
    FROM analysis_events e
    WHERE e.subject_id = subjects.id
);

UPDATE subjects
SET last_activity_at = (
        SELECT e.occurred_at
        FROM analysis_events e
        WHERE e.subject_id = subjects.id
          AND e.appliance_type IS NOT NULL
          AND e.appliance_type <> ''
        ORDER BY e.occurred_at DESC, e.id DESC
        LIMIT 1
    ),
    last_activity_appliance = (
        SELECT e.appliance_type
        FROM analysis_events e
        WHERE e.subject_id = subjects.id
          AND e.appliance_type IS NOT NULL
          AND e.appliance_type <> ''
        ORDER BY e.occurred_at DESC, e.id DESC
        LIMIT 1
    )
WHERE EXISTS (
    SELECT 1
    FROM analysis_events e
    WHERE e.subject_id = subjects.id
      AND e.appliance_type IS NOT NULL
      AND e.appliance_type <> ''
);

CREATE INDEX idx_notifications_event_id
    ON notifications(event_id);

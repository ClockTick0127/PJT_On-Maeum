ALTER TABLE household_profiles
    ADD COLUMN profile_revision BIGINT NOT NULL DEFAULT 1;

ALTER TABLE household_profiles
    ADD COLUMN delivery_mode VARCHAR(20) NOT NULL DEFAULT 'ACTIVE';

ALTER TABLE household_profiles DROP CONSTRAINT chk_household_profile_status;

ALTER TABLE household_profiles
    ADD CONSTRAINT chk_household_profile_status
    CHECK (status IN ('ACTIVE', 'SUPERSEDED', 'REJECTED', 'PENDING', 'SHADOW'));

ALTER TABLE household_profiles
    ADD CONSTRAINT chk_household_profile_revision CHECK (profile_revision >= 1);

ALTER TABLE household_profiles
    ADD CONSTRAINT chk_household_profile_delivery_mode
    CHECK (delivery_mode IN ('ACTIVE', 'SHADOW'));

CREATE INDEX idx_household_profiles_order
    ON household_profiles(household_id, as_of_date DESC, profile_revision DESC);

-- 대상자 상세 화면의 하루 전력 사용 패턴 그래프용 집계 테이블.
-- 구간은 UTC 정각 기준으로 저장하고, 화면 구간(Asia/Seoul 0~23시)은 조회 시 계산한다.
CREATE TABLE hourly_power_usage (
    household_id VARCHAR(255) NOT NULL,
    bucket_start_at TIMESTAMP WITH TIME ZONE NOT NULL,
    energy_wh NUMERIC(18, 6) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_hourly_power_usage
        PRIMARY KEY (household_id, bucket_start_at),
    CONSTRAINT chk_hourly_power_usage_energy_wh
        CHECK (energy_wh >= 0)
);

-- 가전별 분해 결과. 합산 전력량(hourly_power_usage)과 같은 구간 단위를 쓴다.
CREATE TABLE hourly_appliance_power_usage (
    household_id VARCHAR(255) NOT NULL,
    bucket_start_at TIMESTAMP WITH TIME ZONE NOT NULL,
    appliance_type VARCHAR(50) NOT NULL,
    energy_wh NUMERIC(18, 6) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_hourly_appliance_power_usage
        PRIMARY KEY (household_id, bucket_start_at, appliance_type),
    CONSTRAINT chk_hourly_appliance_power_usage_energy_wh
        CHECK (energy_wh >= 0),
    CONSTRAINT chk_hourly_appliance_power_usage_appliance_type
        CHECK (appliance_type IN (
            'KETTLE', 'INDUCTION', 'IRON',
            'MICROWAVE', 'HAIR_DRYER', 'VACUUM_CLEANER'
        ))
);

CREATE INDEX idx_hourly_appliance_power_usage_household_appliance
    ON hourly_appliance_power_usage (household_id, appliance_type);

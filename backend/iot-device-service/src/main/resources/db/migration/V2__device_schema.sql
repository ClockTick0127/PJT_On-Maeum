-- device_db 스키마 v0.1 (ERD 설계서 기준)
-- 원칙: 유저는 Keycloak 소관(keycloak_user_id 값 참조만), 서비스 간 FK 금지(house_id 값 공유),
--       상태·이력 분리, 삭제 대신 상태 전이, 자격증명은 해시만 저장.

CREATE TABLE households (
    house_id       VARCHAR(10)  PRIMARY KEY,
    alias          VARCHAR(50)  NOT NULL,
    grace_minutes  INT          NOT NULL DEFAULT 120,
    away_until     TIMESTAMP WITH TIME ZONE,
    created_at     TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT NOW()
);

CREATE TABLE household_guardians (
    house_id          VARCHAR(10) NOT NULL REFERENCES households (house_id),
    keycloak_user_id  UUID        NOT NULL,  -- Keycloak 참조 (외부 시스템, FK 아님)
    role              VARCHAR(10) NOT NULL DEFAULT 'PRIMARY',
    notify_phone      VARCHAR(20) NOT NULL,
    notify_enabled    BOOLEAN     NOT NULL DEFAULT TRUE,
    PRIMARY KEY (house_id, keycloak_user_id),
    CONSTRAINT chk_guardian_role CHECK (role IN ('PRIMARY', 'SECONDARY'))
);

CREATE TABLE devices (
    device_id      BIGSERIAL    PRIMARY KEY,
    house_id       VARCHAR(10)  NOT NULL REFERENCES households (house_id),
    device_type    VARCHAR(20)  NOT NULL,
    location       VARCHAR(50),
    firmware_ver   VARCHAR(20),
    status         VARCHAR(15)  NOT NULL DEFAULT 'REGISTERED',
    registered_at  TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_device_type   CHECK (device_type IN ('CLAMP', 'PLUG')),
    CONSTRAINT chk_device_status CHECK (status IN ('REGISTERED', 'ACTIVE', 'SUSPENDED', 'RETIRED'))
);

CREATE INDEX idx_devices_house ON devices (house_id);

CREATE TABLE install_history (
    history_id  BIGSERIAL    PRIMARY KEY,
    device_id   BIGINT       NOT NULL REFERENCES devices (device_id),
    event_type  VARCHAR(20)  NOT NULL,
    from_value  VARCHAR(50),
    to_value    VARCHAR(50),
    reason      VARCHAR(200),
    changed_by  VARCHAR(50),
    changed_at  TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_history_event CHECK (event_type IN
        ('INSTALLED', 'RELOCATED', 'STATUS_CHANGED', 'FIRMWARE_UPDATED', 'RETIRED'))
);

CREATE INDEX idx_history_device ON install_history (device_id);

CREATE TABLE device_credentials (
    device_id      BIGINT       PRIMARY KEY REFERENCES devices (device_id),
    mqtt_username  VARCHAR(50)  NOT NULL UNIQUE,
    secret_hash    VARCHAR(200) NOT NULL,   -- BCrypt 해시. 평문은 발급 응답에 1회만 노출
    issued_at      TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT NOW(),
    rotated_at     TIMESTAMP WITH TIME ZONE,
    revoked_at     TIMESTAMP WITH TIME ZONE
);

CREATE TABLE device_acl (
    acl_id         BIGSERIAL     PRIMARY KEY,
    device_id      BIGINT        NOT NULL REFERENCES devices (device_id),
    topic_pattern  VARCHAR(100)  NOT NULL,
    permission     VARCHAR(10)   NOT NULL,
    CONSTRAINT chk_acl_permission CHECK (permission IN ('PUBLISH', 'SUBSCRIBE'))
);

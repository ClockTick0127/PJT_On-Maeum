-- 담당자 가입을 monitoring-service로 흘려보내는 아웃박스.
--
-- "일할 수 있는 담당자" 명단은 monitoring_db.managers에 있고 가입은 device_db에서
-- 일어난다. DB가 갈라져 있어 FK로 묶을 수 없으므로, 가입 트랜잭션에 이벤트를 함께
-- 적어두고 릴레이가 Kafka로 발행한다. 가입이 롤백되면 이벤트도 함께 사라지고,
-- 발행에 실패해도 행이 남아 다시 시도된다(at-least-once).
CREATE TABLE manager_registration_outbox (
    event_id          UUID          PRIMARY KEY,
    -- 한 사람당 한 번만 등록한다. 재가입이 없는 한 이 제약이 중복 발행을 막는다.
    keycloak_user_id  UUID          NOT NULL UNIQUE,
    email             VARCHAR(100)  NOT NULL,
    display_name      VARCHAR(50)   NOT NULL,
    phone             VARCHAR(20),
    organization      VARCHAR(100)  NOT NULL,
    status            VARCHAR(20)   NOT NULL DEFAULT 'PENDING',
    attempt_count     INTEGER       NOT NULL DEFAULT 0,
    next_attempt_at   TIMESTAMP WITH TIME ZONE NOT NULL,
    last_error        TEXT,
    created_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    published_at      TIMESTAMP WITH TIME ZONE,
    CONSTRAINT chk_manager_registration_outbox_status
        CHECK (status IN ('PENDING', 'PUBLISHED'))
);

CREATE INDEX idx_manager_registration_outbox_pending
    ON manager_registration_outbox (status, next_attempt_at);

-- 이미 가입해 있던 기관 소속 계정들도 명단에 올린다.
-- 이 행들이 없으면 기존 복지사는 계속 대상자 등록에서 막힌다.
INSERT INTO manager_registration_outbox (
    event_id, keycloak_user_id, email, display_name, phone, organization,
    status, attempt_count, next_attempt_at, created_at
)
SELECT
    keycloak_user_id, keycloak_user_id, email, display_name, phone, organization,
    'PENDING', 0, NOW(), NOW()
FROM user_profiles
WHERE organization IS NOT NULL
  AND organization <> ''
  AND status <> 'SUSPENDED';

-- 승인 절차를 두지 않기로 했다. 승인 대기로 남아 있던 계정을 정리한다.
UPDATE user_profiles
SET status = 'ACTIVE'
WHERE status = 'PENDING';

-- 계정·가구 멤버십·초대 스키마.
-- 원칙: 신원(비밀번호·세션)은 Keycloak 소관. 여기에는 서비스에서의 프로필과
--       "누가 어느 가구에 어떤 관계로 연결됐는지"만 둔다.

-- 1) 서비스 프로필 — Keycloak 사용자 1명당 1행
CREATE TABLE user_profiles (
    keycloak_user_id  UUID          PRIMARY KEY,
    email             VARCHAR(100)  NOT NULL UNIQUE,
    display_name      VARCHAR(50)   NOT NULL,
    phone             VARCHAR(20),
    organization      VARCHAR(100),          -- 복지사/기관 소속 (일반 사용자는 NULL)
    status            VARCHAR(15)   NOT NULL DEFAULT 'ACTIVE',
    created_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_profile_status CHECK (status IN ('PENDING', 'ACTIVE', 'SUSPENDED'))
);

-- 2) 가구 멤버십 — 기존 household_guardians를 일반화 (보호자 외 본인·복지사도 수용)
ALTER TABLE household_guardians RENAME TO household_members;
ALTER TABLE household_members RENAME COLUMN role TO notify_priority;
ALTER TABLE household_members RENAME CONSTRAINT chk_guardian_role TO chk_member_notify_priority;
ALTER TABLE household_members ADD COLUMN relation VARCHAR(10) NOT NULL DEFAULT 'GUARDIAN';
ALTER TABLE household_members ADD COLUMN joined_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();
ALTER TABLE household_members ADD CONSTRAINT chk_member_relation
    CHECK (relation IN ('SELF', 'GUARDIAN', 'STAFF'));
ALTER TABLE household_members ALTER COLUMN notify_phone DROP NOT NULL;  -- 복지사는 개인 연락처 미등록 가능

CREATE INDEX idx_members_user ON household_members (keycloak_user_id);

-- 3) 초대 코드 — 로그인 수단이 아니라 "가구 접근 신청권"
--    1회용, 만료 있음, 역할이 코드에 박혀 있고, 발급·사용 이력이 남는다.
CREATE TABLE household_invites (
    code          VARCHAR(12)   PRIMARY KEY,
    house_id      VARCHAR(10)   NOT NULL REFERENCES households (house_id),
    relation      VARCHAR(10)   NOT NULL,
    created_by    UUID          NOT NULL,
    created_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at    TIMESTAMP WITH TIME ZONE NOT NULL,
    used_at       TIMESTAMP WITH TIME ZONE,
    used_by       UUID,
    revoked_at    TIMESTAMP WITH TIME ZONE,
    CONSTRAINT chk_invite_relation CHECK (relation IN ('SELF', 'GUARDIAN', 'STAFF'))
);

CREATE INDEX idx_invites_house ON household_invites (house_id);

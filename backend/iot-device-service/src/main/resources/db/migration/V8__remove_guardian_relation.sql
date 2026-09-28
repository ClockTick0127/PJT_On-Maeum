-- 가족·지인 보호자(GUARDIAN) 역할 제거
--
-- 멤버십은 만들 수 있었지만 monitoring 쪽에 대응 개념이 없어 어떤 데이터도 볼 수 없었다
-- (대상자 조회·이벤트·전력 사용량 전부 403). 절반만 구현된 역할을 남겨두면
-- "초대는 되는데 아무것도 안 보이는" 상태라 사용자에게는 고장으로 보인다.
-- 재도입하려면 두 서비스의 권한 모델을 함께 설계해야 한다.
-- 배경: docs/권한모델_소유권_결정요청.md

-- 1) 아직 쓰이지 않은 GUARDIAN 초대는 무효화한다.
--    이미 사용된 코드는 이력이므로 남기고, 아래에서 관계만 STAFF로 옮긴다.
DELETE FROM household_invites
WHERE relation = 'GUARDIAN' AND used_at IS NULL;

-- 2) 사용 이력이 있는 GUARDIAN 초대는 기록 보존을 위해 STAFF로 승계한다.
UPDATE household_invites SET relation = 'STAFF' WHERE relation = 'GUARDIAN';

-- 3) GUARDIAN 멤버십은 삭제한다.
--    어차피 어떤 조회도 통과하지 못했으므로 제거해도 잃는 접근 권한이 없다.
--    STAFF로 승격하면 기관 소속이 아닌 사람에게 담당자 권한을 주게 되므로 하지 않는다.
DELETE FROM household_members WHERE relation = 'GUARDIAN';

-- 4) 허용 값에서 GUARDIAN을 뺀다.
ALTER TABLE household_members DROP CONSTRAINT IF EXISTS chk_member_relation;
ALTER TABLE household_members ADD CONSTRAINT chk_member_relation
    CHECK (relation IN ('SELF', 'STAFF'));

ALTER TABLE household_invites DROP CONSTRAINT IF EXISTS chk_invite_relation;
ALTER TABLE household_invites ADD CONSTRAINT chk_invite_relation
    CHECK (relation IN ('SELF', 'STAFF'));

-- 5) 기본값을 없앤다.
--    SELF(본인)와 STAFF(기관 담당자) 사이에는 합리적인 기본값이 없다.
--    조용히 한쪽으로 정해지면 권한이 잘못 부여되므로 호출자가 반드시 지정하게 한다.
ALTER TABLE household_members ALTER COLUMN relation DROP DEFAULT;

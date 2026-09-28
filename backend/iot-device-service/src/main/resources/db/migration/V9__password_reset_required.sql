-- 초기 비밀번호 사용 중임을 표시한다.
--
-- 담당자가 대상자 계정을 만들면서 초기 비밀번호를 발급해 전달한다.
-- 그 비밀번호는 담당자도 알고 있으므로, 대상자가 바꾸기 전까지는
-- "본인이 응답했다"는 기록의 신뢰도가 낮다.
--
-- Keycloak의 temporary=true를 쓰지 않는 이유: UPDATE_PASSWORD 필수 조치가 걸리면
-- password grant(직접 승인) 로그인이 "Account is not fully set up"으로 실패한다.
-- 우리는 직접 승인 방식이라 필수 조치를 처리할 화면이 없다. 그래서 Keycloak에는
-- 정상 비밀번호로 넣고, 변경 유도는 이 플래그로 서비스가 직접 한다.

ALTER TABLE user_profiles
    ADD COLUMN password_reset_required BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN user_profiles.password_reset_required IS
    '초기 비밀번호 사용 중. 본인이 변경하면 false. 이 동안의 응답은 대리 응답 가능성이 있다.';

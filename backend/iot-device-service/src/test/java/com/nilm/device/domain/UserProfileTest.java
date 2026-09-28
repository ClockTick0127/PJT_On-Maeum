package com.nilm.device.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class UserProfileTest {

    private static final UUID USER = UUID.randomUUID();

    private UserProfile profile(String organization) {
        return new UserProfile(USER, "user@nilm.test", "홍길동", "010-1234-5678", organization);
    }

    @Test
    @DisplayName("승인 절차를 두지 않으므로 기관 소속 가입도 바로 활성이다")
    void staffStartsActive() {
        assertEquals(UserProfile.Status.ACTIVE, profile("행복구 복지센터").getStatus());
    }

    @Test
    @DisplayName("기관 소속이 없는 가입도 활성이다")
    void generalUserStartsActive() {
        assertEquals(UserProfile.Status.ACTIVE, profile(null).getStatus());
    }

    @Test
    @DisplayName("정지하면 중단 상태가 된다")
    void suspend() {
        UserProfile p = profile(null);

        p.suspend();

        assertEquals(UserProfile.Status.SUSPENDED, p.getStatus());
    }

    @Test
    @DisplayName("프로필 수정에서 null 필드는 기존 값을 유지한다")
    void updateKeepsNullFields() {
        UserProfile p = profile(null);

        p.updateProfile("김철수", null);

        assertEquals("김철수", p.getDisplayName());
        assertEquals("010-1234-5678", p.getPhone(), "전화번호를 보내지 않았으므로 유지되어야 한다");
    }

    @Test
    @DisplayName("이름을 공백으로 보내도 기존 이름을 지우지 않는다")
    void blankNameIsIgnored() {
        UserProfile p = profile(null);

        p.updateProfile("   ", null);

        assertEquals("홍길동", p.getDisplayName());
    }

    @Test
    @DisplayName("전화번호는 빈 문자열로 지울 수 있다 — 삭제 의사와 미변경을 구분한다")
    void blankPhoneClearsValue() {
        UserProfile p = profile(null);

        p.updateProfile(null, "");

        assertNull(p.getPhone());
        assertEquals("홍길동", p.getDisplayName(), "이름은 건드리지 않아야 한다");
    }

    @Test
    @DisplayName("프로필 수정으로 기관 소속은 바뀌지 않는다 — 담당자 명단과 엮여 있어 별도 절차다")
    void updateDoesNotTouchOrganization() {
        UserProfile p = profile("행복구 복지센터");

        p.updateProfile("김철수", "010-0000-0000");

        assertEquals("행복구 복지센터", p.getOrganization());
    }
}

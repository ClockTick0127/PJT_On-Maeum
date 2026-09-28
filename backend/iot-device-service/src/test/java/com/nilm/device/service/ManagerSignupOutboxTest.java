package com.nilm.device.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.nilm.device.api.dto.AuthDtos;
import com.nilm.device.common.DuplicateResourceException;
import com.nilm.device.domain.ManagerRegistrationOutbox;
import com.nilm.device.domain.UserProfile;
import com.nilm.device.repository.ManagerRegistrationOutboxRepository;
import com.nilm.device.repository.UserProfileRepository;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * 가입한 복지사가 monitoring의 담당자 명단까지 닿는지 — 그 첫 구간인 아웃박스 적재.
 * 브로커는 없으므로 발행까지는 보지 않는다.
 */
@SpringBootTest
class ManagerSignupOutboxTest {

    @Autowired AuthService authService;
    @Autowired UserProfileRepository profiles;
    @Autowired ManagerRegistrationOutboxRepository outbox;

    @MockitoBean KeycloakAdminClient keycloak;

    @BeforeEach
    void setup() {
        outbox.deleteAll();
        profiles.deleteAll();
        when(keycloak.createUser(anyString(), anyString(), anyString()))
                .thenAnswer(invocation -> UUID.randomUUID());
    }

    private AuthDtos.SignupRequest signup(String email, String organization) {
        return new AuthDtos.SignupRequest(email, "비밀번호1234", "홍길동", "01012345678", organization);
    }

    @Test
    void 기관_소속_가입은_담당자_등록_이벤트를_남긴다() {
        authService.signup(signup("welfare@nilm.local", "행복복지관"));

        List<ManagerRegistrationOutbox> rows = outbox.findAll();
        assertThat(rows).hasSize(1);

        ManagerRegistrationOutbox row = rows.get(0);
        assertThat(row.getStatus()).isEqualTo(ManagerRegistrationOutbox.Status.PENDING);
        assertThat(row.getOrganization()).isEqualTo("행복복지관");
        assertThat(row.getDisplayName()).isEqualTo("홍길동");
        assertThat(row.getEmail()).isEqualTo("welfare@nilm.local");
        assertThat(row.getPublishedAt()).isNull();
        assertThat(row.getAttemptCount()).isZero();

        // 이벤트의 주인은 Keycloak 사용자다. 받는 쪽이 이 값으로 담당자를 찾는다.
        UserProfile profile = profiles.findAll().get(0);
        assertThat(row.getKeycloakUserId()).isEqualTo(profile.getKeycloakUserId());
    }

    @Test
    void 기관_소속이_없는_가입은_담당자가_아니므로_내보내지_않는다() {
        authService.signup(signup("guardian@nilm.local", null));

        assertThat(outbox.findAll()).isEmpty();
    }

    @Test
    void 승인_절차가_없으므로_기관_가입도_바로_활성이다() {
        authService.signup(signup("welfare2@nilm.local", "행복복지관"));

        assertThat(profiles.findAll())
                .singleElement()
                .extracting(UserProfile::getStatus)
                .isEqualTo(UserProfile.Status.ACTIVE);
    }

    @Test
    void 막힌_중복_가입은_이벤트를_남기지_않는다() {
        authService.signup(signup("welfare3@nilm.local", "행복복지관"));

        // 같은 이메일은 중복으로 막힌다. 이벤트만 더 쌓여 담당자가 두 번 등록되면 안 된다.
        assertThatThrownBy(() -> authService.signup(signup("welfare3@nilm.local", "행복복지관")))
                .isInstanceOf(DuplicateResourceException.class);

        assertThat(outbox.findAll()).hasSize(1);
        assertThat(profiles.findAll()).hasSize(1);
    }
}

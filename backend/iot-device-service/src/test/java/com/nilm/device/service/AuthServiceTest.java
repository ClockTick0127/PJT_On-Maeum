package com.nilm.device.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.nilm.device.api.dto.AuthDtos;
import com.nilm.device.common.ForbiddenException;
import com.nilm.device.common.InvalidOperationException;
import com.nilm.device.common.NotFoundException;
import com.nilm.device.domain.UserProfile;
import com.nilm.device.repository.ManagerRegistrationOutboxRepository;
import com.nilm.device.repository.UserProfileRepository;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * 인증 서비스 — Keycloak 호출 순서와 실패 시 처리가 검증 대상이다.
 * 토큰 발급 자체는 Keycloak 책임이므로 여기서 확인하지 않는다.
 */
@ExtendWith(MockitoExtension.class)
class AuthServiceTest {

    private static final UUID USER = UUID.randomUUID();
    private static final String EMAIL = "user@nilm.test";

    @Mock
    private KeycloakAdminClient keycloak;
    @Mock
    private UserProfileRepository profileRepository;
    @Mock
    private ManagerRegistrationOutboxRepository outboxRepository;
    @Mock
    private MembershipService membershipService;
    @Mock
    private ReadableSecretGenerator secretGenerator;

    @InjectMocks
    private AuthService authService;

    private UserProfile profile() {
        return new UserProfile(USER, EMAIL, "홍길동", "010-1234-5678", null);
    }

    // ── 이메일 중복 확인 ────────────────────────────────────

    @Test
    @DisplayName("대소문자를 달리 입력해도 같은 이메일로 판정한다")
    void checkEmailNormalizesCase() {
        when(profileRepository.existsByEmail(EMAIL)).thenReturn(true);

        AuthDtos.EmailAvailability result = authService.checkEmail("  User@NILM.test  ");

        assertEquals(EMAIL, result.email(), "정규화된 형태를 돌려줘야 한다");
        assertFalse(result.available());
    }

    @Test
    @DisplayName("등록되지 않은 이메일은 사용 가능하다")
    void checkEmailAvailable() {
        when(profileRepository.existsByEmail(EMAIL)).thenReturn(false);

        assertTrue(authService.checkEmail(EMAIL).available());
    }

    // ── 로그아웃 ──────────────────────────────────────────

    @Test
    @DisplayName("로그아웃은 refresh token을 Keycloak에서 폐기한다")
    void logoutRevokesRefreshToken() {
        authService.logout(new AuthDtos.LogoutRequest("refresh-token"));

        verify(keycloak).logout("refresh-token");
    }

    // ── 프로필 수정 ────────────────────────────────────────

    @Test
    @DisplayName("이름을 바꾸면 Keycloak을 먼저 갱신한 뒤 로컬 프로필에 반영한다")
    void updateProfileSyncsKeycloakFirst() {
        UserProfile profile = profile();
        when(profileRepository.findById(USER)).thenReturn(Optional.of(profile));

        authService.updateProfile(USER, new AuthDtos.UpdateProfileRequest("김철수", "010-9999-9999"));

        InOrder order = Mockito.inOrder(keycloak);
        order.verify(keycloak).updateDisplayName(USER, "김철수");
        assertEquals("김철수", profile.getDisplayName());
        assertEquals("010-9999-9999", profile.getPhone());
    }

    @Test
    @DisplayName("이름을 보내지 않으면 Keycloak을 호출하지 않는다")
    void updateProfileSkipsKeycloakWhenNameUnchanged() {
        UserProfile profile = profile();
        when(profileRepository.findById(USER)).thenReturn(Optional.of(profile));

        authService.updateProfile(USER, new AuthDtos.UpdateProfileRequest(null, "010-9999-9999"));

        verify(keycloak, never()).updateDisplayName(any(), anyString());
        assertEquals("홍길동", profile.getDisplayName());
        assertEquals("010-9999-9999", profile.getPhone());
    }

    @Test
    @DisplayName("없는 프로필은 404다")
    void updateProfileNotFound() {
        when(profileRepository.findById(USER)).thenReturn(Optional.empty());

        assertThrows(NotFoundException.class, () ->
                authService.updateProfile(USER, new AuthDtos.UpdateProfileRequest("김철수", null)));
    }

    // ── 대상자 계정 대리 생성 ────────────────────────────────

    private UserProfile staffProfile() {
        return new UserProfile(USER, "staff@nilm.test", "김복지", null, "행복구 복지센터");
    }

    @Test
    @DisplayName("담당자는 대상자 계정을 만들고 초기 비밀번호를 1회 받아 간다")
    void proxyUserIssuesInitialPassword() {
        when(profileRepository.findById(USER)).thenReturn(Optional.of(staffProfile()));
        when(profileRepository.existsByEmail(anyString())).thenReturn(false);
        when(secretGenerator.generate()).thenReturn("KMPX-4R7T-BJQW");
        when(keycloak.createUser(anyString(), eq("KMPX-4R7T-BJQW"), eq("박어르신")))
                .thenReturn(UUID.randomUUID());
        when(profileRepository.save(any(UserProfile.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        AuthDtos.ProxyUserResponse result = authService.createProxyUser(
                USER, new AuthDtos.ProxyUserRequest("박어르신", "010-2222-3333", null));

        assertEquals("KMPX-4R7T-BJQW", result.initialPassword(), "담당자가 전달할 값이라 1회 노출한다");
        assertTrue(result.profile().passwordResetRequired(), "본인이 바꾸기 전까지 켜져 있어야 한다");
        assertNull(result.profile().organization(), "대상자는 담당자가 아니므로 소속을 남기지 않는다");
    }

    @Test
    @DisplayName("이메일을 주지 않으면 내부용 주소를 만든다 — 보낼 수 없는 주소임이 드러나야 한다")
    void proxyUserGetsInternalEmail() {
        when(profileRepository.findById(USER)).thenReturn(Optional.of(staffProfile()));
        when(profileRepository.existsByEmail(anyString())).thenReturn(false);
        when(secretGenerator.generate()).thenReturn("KMPX-4R7T-BJQW");
        when(keycloak.createUser(anyString(), anyString(), anyString()))
                .thenReturn(UUID.randomUUID());
        when(profileRepository.save(any(UserProfile.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        AuthDtos.ProxyUserResponse result = authService.createProxyUser(
                USER, new AuthDtos.ProxyUserRequest("박어르신", null, null));

        assertTrue(result.profile().email().endsWith("@no-email.nilm.local"),
                result.profile().email());
    }

    @Test
    @DisplayName("기관 소속이 아니면 남의 계정을 만들 수 없다")
    void proxyUserRejectedForNonStaff() {
        when(profileRepository.findById(USER)).thenReturn(Optional.of(
                new UserProfile(USER, EMAIL, "홍길동", null, null)));

        assertThrows(ForbiddenException.class, () -> authService.createProxyUser(
                USER, new AuthDtos.ProxyUserRequest("박어르신", null, null)));

        verify(keycloak, never()).createUser(anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("본인이 비밀번호를 바꾸면 초기 비밀번호 표시가 내려간다")
    void changePasswordClearsResetFlag() {
        UserProfile profile = new UserProfile(USER, EMAIL, "박어르신", null, null, true);
        when(profileRepository.findById(USER)).thenReturn(Optional.of(profile));

        authService.changePassword(USER, new AuthDtos.ChangePasswordRequest("KMPX-4R7T-BJQW", "my-own-pass"));

        assertFalse(profile.isPasswordResetRequired());
    }

    // ── 비밀번호 변경 ──────────────────────────────────────

    @Test
    @DisplayName("현재 비밀번호를 확인한 뒤 재설정하고 모든 세션을 끊는다")
    void changePasswordVerifiesThenResetsAndLogsOut() {
        when(profileRepository.findById(USER)).thenReturn(Optional.of(profile()));

        authService.changePassword(USER, new AuthDtos.ChangePasswordRequest("old-pass", "new-pass"));

        InOrder order = Mockito.inOrder(keycloak);
        order.verify(keycloak).login(EMAIL, "old-pass");
        order.verify(keycloak).resetPassword(USER, "new-pass");
        order.verify(keycloak).logoutAllSessions(USER);
    }

    @Test
    @DisplayName("현재 비밀번호가 틀리면 재설정하지 않는다")
    void changePasswordRejectsWrongCurrent() {
        when(profileRepository.findById(USER)).thenReturn(Optional.of(profile()));
        when(keycloak.login(eq(EMAIL), eq("wrong")))
                .thenThrow(new InvalidOperationException("이메일 또는 비밀번호가 올바르지 않습니다"));

        InvalidOperationException e = assertThrows(InvalidOperationException.class, () ->
                authService.changePassword(USER, new AuthDtos.ChangePasswordRequest("wrong", "new-pass")));

        assertTrue(e.getMessage().contains("현재 비밀번호"));
        verify(keycloak, never()).resetPassword(any(), anyString());
        verify(keycloak, never()).logoutAllSessions(any());
    }

    @Test
    @DisplayName("같은 비밀번호로는 바꿀 수 없다 — 확인 요청조차 보내지 않는다")
    void changePasswordRejectsSameValue() {
        when(profileRepository.findById(USER)).thenReturn(Optional.of(profile()));

        assertThrows(InvalidOperationException.class, () ->
                authService.changePassword(USER, new AuthDtos.ChangePasswordRequest("same", "same")));

        verify(keycloak, never()).login(anyString(), anyString());
        verify(keycloak, never()).resetPassword(any(), anyString());
    }
}

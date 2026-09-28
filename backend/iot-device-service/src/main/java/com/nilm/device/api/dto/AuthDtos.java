package com.nilm.device.api.dto;

import com.nilm.device.domain.UserProfile;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.util.List;
import java.util.UUID;

public final class AuthDtos {

    private AuthDtos() {
    }

    public record SignupRequest(
            @NotBlank @Email @Size(max = 100)
            String email,
            @NotBlank @Size(min = 8, max = 72, message = "비밀번호는 8자 이상이어야 합니다")
            String password,
            @NotBlank @Size(max = 50)
            String displayName,
            @Size(max = 20)
            String phone,
            /** 기관 소속이면 입력 — 복지사 가입으로 간주되어 담당자 명단에 오른다 */
            @Size(max = 100)
            String organization
    ) {
    }

    public record LoginRequest(
            @NotBlank @Email
            String email,
            @NotBlank
            String password
    ) {
    }

    public record RefreshRequest(
            @NotBlank
            String refreshToken
    ) {
    }

    /** 로그아웃 — 서버가 refresh token을 폐기해야 실제로 세션이 끊긴다. */
    public record LogoutRequest(
            @NotBlank
            String refreshToken
    ) {
    }

    /** 프로필 부분 수정 — null 필드는 기존 값을 유지한다. */
    public record UpdateProfileRequest(
            @Size(max = 50)
            String displayName,
            @Size(max = 20)
            String phone
    ) {
    }

    public record ChangePasswordRequest(
            @NotBlank
            String currentPassword,
            @NotBlank @Size(min = 8, max = 72, message = "비밀번호는 8자 이상이어야 합니다")
            String newPassword
    ) {
    }

    /** 가입 폼에서 제출 전에 확인한다 — 다 채우고 나서 409를 만나지 않도록. */
    public record EmailAvailability(
            String email,
            boolean available
    ) {
    }

    /**
     * 대리 계정 생성 — 담당자가 대상자 몫으로 신원만 만든다.
     * 이메일은 선택이다. 어르신 대부분은 이메일이 없으므로 없으면 내부용을 만들어 쓴다.
     */
    public record ProxyUserRequest(
            @NotBlank @Size(max = 50)
            String displayName,
            @Size(max = 20)
            String phone,
            @Email @Size(max = 100)
            String email
    ) {
    }

    /**
     * 대리 생성 응답 — {@code initialPassword}는 이 응답에서 1회만 노출된다.
     * 서버에는 Keycloak 해시만 남으므로 분실하면 재발급해야 한다.
     * 담당자가 대상자에게 구두·서면으로 전달하는 값이라 불러줄 수 있는 형태로 만든다.
     */
    public record ProxyUserResponse(
            ProfileResponse profile,
            String initialPassword
    ) {
    }

    public record TokenResponse(
            String accessToken,
            String refreshToken,
            Integer expiresIn,
            String tokenType
    ) {
        public static TokenResponse of(String access, String refresh, Integer expiresIn) {
            return new TokenResponse(access, refresh, expiresIn, "Bearer");
        }
    }

    public record ProfileResponse(
            UUID userId,
            String email,
            String displayName,
            String phone,
            String organization,
            UserProfile.Status status,
            /** 초기 비밀번호 사용 중 — 프론트는 이 값이 true면 변경 화면으로 보낸다. */
            boolean passwordResetRequired
    ) {
        public static ProfileResponse from(UserProfile p) {
            return new ProfileResponse(p.getKeycloakUserId(), p.getEmail(), p.getDisplayName(),
                    p.getPhone(), p.getOrganization(), p.getStatus(), p.isPasswordResetRequired());
        }
    }

    /** 로그인 직후 화면 구성의 기준 — 내 프로필 + 내가 접근 가능한 가구 목록 */
    public record MeResponse(
            ProfileResponse profile,
            List<MemberDtos.MyHousehold> households
    ) {
    }
}

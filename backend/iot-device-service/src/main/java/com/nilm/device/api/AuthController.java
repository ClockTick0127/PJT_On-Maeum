package com.nilm.device.api;

import com.nilm.device.api.dto.AuthDtos;
import com.nilm.device.security.CurrentUser;
import com.nilm.device.service.AuthService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "Auth", description = "가입·로그인 (신원은 Keycloak, 서비스 프로필은 device_db)")
@RestController
@Validated
@RequestMapping("/api/auth")
public class AuthController {

    private final AuthService authService;
    private final CurrentUser currentUser;

    public AuthController(AuthService authService, CurrentUser currentUser) {
        this.authService = authService;
        this.currentUser = currentUser;
    }

    @Operation(summary = "회원가입",
            description = "Keycloak 계정 생성 + 서비스 프로필 저장. organization을 입력하면 "
                    + "기관 담당자 가입으로 간주되어 모니터링의 담당자 명단에 등록된다.")
    @PostMapping("/signup")
    @ResponseStatus(HttpStatus.CREATED)
    public AuthDtos.ProfileResponse signup(@Valid @RequestBody AuthDtos.SignupRequest request) {
        return authService.signup(request);
    }

    @Operation(summary = "로그인", description = "Keycloak 토큰 발급 대행. 비밀번호는 저장하지 않는다.")
    @PostMapping("/login")
    public AuthDtos.TokenResponse login(@Valid @RequestBody AuthDtos.LoginRequest request) {
        return authService.login(request);
    }

    @Operation(summary = "토큰 갱신")
    @PostMapping("/refresh")
    public AuthDtos.TokenResponse refresh(@Valid @RequestBody AuthDtos.RefreshRequest request) {
        return authService.refresh(request);
    }

    @Operation(summary = "로그아웃",
            description = "refresh token을 Keycloak에서 폐기한다. 클라이언트가 토큰을 지우는 것만으로는 "
                    + "서버 세션이 남아 유출된 토큰으로 갱신이 가능하다. 이미 무효한 토큰이어도 204.")
    @PostMapping("/logout")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void logout(@Valid @RequestBody AuthDtos.LogoutRequest request) {
        authService.logout(request);
    }

    @Operation(summary = "이메일 사용 가능 여부",
            description = "가입 폼에서 제출 전에 확인한다. 최종 판정은 가입 시점이며, "
                    + "그 사이 선점되면 signup이 409로 응답한다.")
    @GetMapping("/check-email")
    public AuthDtos.EmailAvailability checkEmail(
            @RequestParam @NotBlank @Email String email) {
        return authService.checkEmail(email);
    }

    @Operation(summary = "대상자 계정 대리 생성",
            description = "담당자가 대상자 몫으로 계정을 만들고 초기 비밀번호를 받아 간다. "
                    + "initialPassword는 이 응답에서 1회만 노출되며 담당자가 대상자에게 전달한다. "
                    + "대상자가 바꾸기 전까지 passwordResetRequired가 켜져 있다. "
                    + "이메일을 주지 않으면 내부용 주소를 생성한다. 기관 소속 계정만 호출할 수 있다.")
    @PostMapping("/users")
    @ResponseStatus(HttpStatus.CREATED)
    public AuthDtos.ProxyUserResponse createProxyUser(
            @Valid @RequestBody AuthDtos.ProxyUserRequest request) {
        return authService.createProxyUser(currentUser.id(), request);
    }

    @Operation(summary = "내 정보",
            description = "프로필 + 내가 접근 가능한 가구 목록(관계 포함). 로그인 직후 화면 구성의 기준.")
    @GetMapping("/me")
    public AuthDtos.MeResponse me() {
        return authService.me(currentUser.id());
    }

    @Operation(summary = "내 정보 수정",
            description = "이름·전화번호 부분 수정 (null 필드는 유지). 이름은 Keycloak에도 반영된다. "
                    + "이메일과 기관 소속은 여기서 바꿀 수 없다.")
    @PatchMapping("/me")
    public AuthDtos.ProfileResponse updateMe(
            @Valid @RequestBody AuthDtos.UpdateProfileRequest request) {
        return authService.updateProfile(currentUser.id(), request);
    }

    @Operation(summary = "비밀번호 변경",
            description = "현재 비밀번호를 확인한 뒤 변경하고, 해당 사용자의 모든 세션을 끊는다. "
                    + "호출자 본인도 새 비밀번호로 다시 로그인해야 한다.")
    @PostMapping("/password")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void changePassword(@Valid @RequestBody AuthDtos.ChangePasswordRequest request) {
        authService.changePassword(currentUser.id(), request);
    }
}

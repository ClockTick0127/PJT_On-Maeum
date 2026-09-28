package com.nilm.device.service;

import com.nilm.device.common.DuplicateResourceException;
import com.nilm.device.common.InvalidOperationException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClient;

/**
 * Keycloak 연동 — 신원(계정·비밀번호·토큰)은 전적으로 Keycloak이 소유한다.
 *
 * <p>두 가지 자격으로 호출한다:
 * <ul>
 *   <li>서비스 계정(client_credentials): 사용자 생성·삭제 등 Admin API</li>
 *   <li>사용자 자격(password grant): 로그인 대행 — 비밀번호는 통과만 하고 저장하지 않는다</li>
 * </ul>
 */
@Component
public class KeycloakAdminClient {

    private static final Logger log = LoggerFactory.getLogger(KeycloakAdminClient.class);

    private static final String GRANT_PASSWORD = "password";
    private static final String GRANT_REFRESH_TOKEN = "refresh_token";

    public record TokenResponse(String accessToken, String refreshToken, Integer expiresIn) {
    }

    private final RestClient http;
    private final String realm;
    private final String clientId;
    private final String clientSecret;

    public KeycloakAdminClient(
            @Value("${app.keycloak.server-url:http://localhost:8090}") String serverUrl,
            @Value("${app.keycloak.realm:nilm}") String realm,
            @Value("${app.keycloak.client-id:nilm-backend}") String clientId,
            @Value("${app.keycloak.client-secret:}") String clientSecret) {
        this.http = RestClient.builder().baseUrl(serverUrl).build();
        this.realm = realm;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    /** 사용자 생성 후 Keycloak user id 반환. 이메일이 이미 있으면 409. */
    public UUID createUser(String email, String password, String displayName) {
        String adminToken = serviceAccountToken();
        Map<String, Object> body = new LinkedHashMap<>(Map.of(
                "username", email,
                "email", email,
                "firstName", displayName,
                // Keycloak 기본 사용자 프로필은 성·이름을 모두 필수로 요구하며
                // 비어 있으면 로그인 시 "Account is not fully set up"으로 거부된다.
                // 서비스의 이름 원본은 user_profiles.display_name이므로 자리만 채운다.
                "lastName", "-",
                "enabled", true,
                "emailVerified", false));
        // temporary=true는 쓰지 않는다 — UPDATE_PASSWORD 필수 조치가 걸리면
        // password grant 로그인이 "Account is not fully set up"으로 실패한다.
        body.put("credentials", List.of(Map.of(
                "type", GRANT_PASSWORD,
                "value", password,
                "temporary", false)));
        URI location = http.post()
                .uri("/admin/realms/{realm}/users", realm)
                .header("Authorization", "Bearer " + adminToken)
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .exchange((request, response) -> {
                    HttpStatusCode status = response.getStatusCode();
                    if (status.value() == 409) {
                        throw new DuplicateResourceException("계정", email);
                    }
                    if (status.isError()) {
                        log.error("Keycloak 사용자 생성 실패: {} {}", status, response.bodyTo(String.class));
                        throw new InvalidOperationException("계정 생성에 실패했습니다");
                    }
                    return response.getHeaders().getLocation();
                });

        if (location == null) {
            throw new InvalidOperationException("계정 생성 응답에서 사용자 식별자를 찾지 못했습니다");
        }
        String path = location.getPath();
        return UUID.fromString(path.substring(path.lastIndexOf('/') + 1));
    }

    /** 로컬 저장 실패 시 보상 — 생성된 Keycloak 사용자를 되돌린다. */
    public void deleteUser(UUID userId) {
        try {
            http.delete()
                    .uri("/admin/realms/{realm}/users/{id}", realm, userId)
                    .header("Authorization", "Bearer " + serviceAccountToken())
                    .retrieve()
                    .toBodilessEntity();
        } catch (RuntimeException e) {
            // 보상 실패는 가입 실패 원인을 덮지 않도록 로그만 남긴다 (고아 계정은 운영에서 정리)
            log.error("보상 삭제 실패 — 고아 Keycloak 계정: {}", userId, e);
        }
    }

    /** 로그인 — 사용자 자격증명을 Keycloak에 위임하고 토큰만 받아온다. */
    public TokenResponse login(String email, String password) {
        MultiValueMap<String, String> form = form(GRANT_PASSWORD);
        form.add("username", email);
        form.add(GRANT_PASSWORD, password);
        return token(form, "이메일 또는 비밀번호가 올바르지 않습니다");
    }

    public TokenResponse refresh(String refreshToken) {
        MultiValueMap<String, String> form = form(GRANT_REFRESH_TOKEN);
        form.add(GRANT_REFRESH_TOKEN, refreshToken);
        return token(form, "다시 로그인해 주세요");
    }

    /**
     * 로그아웃 — refresh token을 Keycloak에서 폐기한다.
     *
     * <p>클라이언트가 저장소에서 토큰을 지우는 것만으로는 refresh token이 살아 있어,
     * 유출된 토큰으로 계속 갱신할 수 있다. 서버가 폐기해야 실제로 끊긴다.
     * 이미 만료·폐기된 토큰이어도 목적은 달성된 상태이므로 오류로 보지 않는다.
     */
    public void logout(String refreshToken) {
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("client_id", clientId);
        form.add("client_secret", clientSecret);
        form.add(GRANT_REFRESH_TOKEN, refreshToken);
        http.post()
                .uri("/realms/{realm}/protocol/openid-connect/logout", realm)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .body(form)
                .exchange((request, response) -> {
                    if (response.getStatusCode().isError()) {
                        log.debug("로그아웃 응답 {} — 이미 무효한 토큰일 수 있다",
                                response.getStatusCode());
                    }
                    return null;
                });
    }

    /** 표시 이름 변경을 Keycloak에도 반영한다(firstName). 서비스의 원본은 user_profiles다. */
    public void updateDisplayName(UUID userId, String displayName) {
        adminWrite(HttpMethod.PUT, "/admin/realms/{realm}/users/{id}",
                userId, Map.of("firstName", displayName), "이름 변경에 실패했습니다");
    }

    /** 비밀번호 재설정 — 현재 비밀번호 확인은 호출자가 먼저 끝낸다. */
    public void resetPassword(UUID userId, String newPassword) {
        adminWrite(HttpMethod.PUT, "/admin/realms/{realm}/users/{id}/reset-password",
                userId,
                Map.of("type", GRANT_PASSWORD, "value", newPassword, "temporary", false),
                "비밀번호 변경에 실패했습니다");
    }

    /**
     * 해당 사용자의 모든 세션을 끊는다.
     * 비밀번호를 바꾼 뒤에도 이전 비밀번호로 받은 토큰이 살아 있으면 변경의 의미가 없다.
     */
    public void logoutAllSessions(UUID userId) {
        adminWrite(HttpMethod.POST, "/admin/realms/{realm}/users/{id}/logout",
                userId, Map.of(), "세션 정리에 실패했습니다");
    }

    private void adminWrite(HttpMethod method, String uri, UUID userId,
                            Map<String, Object> body, String failureMessage) {
        http.method(method)
                .uri(uri, realm, userId)
                .header("Authorization", "Bearer " + serviceAccountToken())
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .exchange((request, response) -> {
                    if (response.getStatusCode().isError()) {
                        log.error("Keycloak 관리 요청 실패: {} {} {}", method,
                                response.getStatusCode(), response.bodyTo(String.class));
                        throw new InvalidOperationException(failureMessage);
                    }
                    return null;
                });
    }

    private String serviceAccountToken() {
        return token(form("client_credentials"), "인증 서버와 통신할 수 없습니다").accessToken();
    }

    private MultiValueMap<String, String> form(String grantType) {
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("grant_type", grantType);
        form.add("client_id", clientId);
        form.add("client_secret", clientSecret);
        return form;
    }

    @SuppressWarnings("unchecked")
    private TokenResponse token(MultiValueMap<String, String> form, String failureMessage) {
        Map<String, Object> body = http.post()
                .uri("/realms/{realm}/protocol/openid-connect/token", realm)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .body(form)
                .exchange((request, response) -> {
                    if (response.getStatusCode().isError()) {
                        log.warn("토큰 요청 실패: {}", response.getStatusCode());
                        throw new InvalidOperationException(failureMessage);
                    }
                    return (Map<String, Object>) response.bodyTo(Map.class);
                });
        if (body == null) {
            throw new InvalidOperationException(failureMessage);
        }
        return new TokenResponse(
                (String) body.get("access_token"),
                (String) body.get(GRANT_REFRESH_TOKEN),
                (Integer) body.get("expires_in"));
    }
}

package com.nilm.monitoring.config;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

/**
 * Calls the real account and household APIs when local test data is enabled.
 * Passwords and access tokens are never logged or persisted by this service.
 */
@Component
@ConditionalOnProperty(name = "app.test-data.enabled", havingValue = "true")
public class TestDataApiClient {

    private final RestClient http;

    public TestDataApiClient(
            RestClient.Builder builder,
            @Value("${app.test-data.device-service-url:http://localhost:8081}")
            String deviceServiceUrl
    ) {
        this.http = builder.baseUrl(deviceServiceUrl).build();
    }

    public SeedAccount ensureUser(
            String email,
            String password,
            String displayName,
            String phone,
            String organization
    ) {
        UUID createdUserId = signup(
                new SignupRequest(email, password, displayName, phone, organization)
        );
        TokenResponse token = http.post()
                .uri("/api/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new LoginRequest(email, password))
                .retrieve()
                .body(TokenResponse.class);
        if (token == null || token.accessToken() == null
                || token.accessToken().isBlank()) {
            throw new IllegalStateException("테스트 계정 로그인 응답에 토큰이 없습니다: " + email);
        }

        MeResponse me = http.get()
                .uri("/api/auth/me")
                .headers(headers -> headers.setBearerAuth(token.accessToken()))
                .retrieve()
                .body(MeResponse.class);
        if (me == null || me.profile() == null || me.profile().userId() == null) {
            throw new IllegalStateException("테스트 계정 조회 응답에 사용자 ID가 없습니다: " + email);
        }
        if (createdUserId != null && !createdUserId.equals(me.profile().userId())) {
            throw new IllegalStateException("회원가입과 로그인의 사용자 ID가 다릅니다: " + email);
        }

        Map<String, String> households = me.households() == null
                ? Map.of()
                : me.households().stream()
                        .collect(Collectors.toUnmodifiableMap(
                                MyHousehold::houseId,
                                MyHousehold::relation
                        ));
        return new SeedAccount(
                me.profile().userId(),
                token.accessToken(),
                households
        );
    }

    public void ensureHousehold(
            SeedAccount account,
            String houseId,
            String alias,
            int graceMinutes
    ) {
        String existingRelation = account.householdRelations().get(houseId);
        if (existingRelation != null) {
            if (!"SELF".equals(existingRelation)) {
                throw new IllegalStateException(
                        "테스트 대상자가 가구의 SELF 멤버가 아닙니다: " + houseId
                );
            }
            return;
        }
        try {
            http.post()
                    .uri("/api/devices/households")
                    .headers(headers -> headers.setBearerAuth(account.accessToken()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(new HouseholdCreateRequest(houseId, alias, graceMinutes))
                    .retrieve()
                    .toBodilessEntity();
        } catch (HttpClientErrorException.Conflict e) {
            throw new IllegalStateException(
                    "가구 ID가 다른 사용자에게 이미 등록되어 있습니다: " + houseId,
                    e
            );
        }
    }

    private UUID signup(SignupRequest request) {
        try {
            ProfileResponse response = http.post()
                    .uri("/api/auth/signup")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(request)
                    .retrieve()
                    .body(ProfileResponse.class);
            if (response == null || response.userId() == null) {
                throw new IllegalStateException(
                        "회원가입 응답에 사용자 ID가 없습니다: " + request.email()
                );
            }
            return response.userId();
        } catch (HttpClientErrorException.Conflict ignored) {
            // A previous start may already have created the account. Login below
            // proves that the configured credentials still own that account.
            return null;
        }
    }

    public record SeedAccount(
            UUID userId,
            String accessToken,
            Map<String, String> householdRelations
    ) {
    }

    private record SignupRequest(
            String email,
            String password,
            String displayName,
            String phone,
            String organization
    ) {
    }

    private record LoginRequest(String email, String password) {
    }

    private record HouseholdCreateRequest(
            String houseId,
            String alias,
            Integer graceMinutes
    ) {
    }

    private record TokenResponse(
            String accessToken,
            String refreshToken,
            Integer expiresIn,
            String tokenType
    ) {
    }

    private record ProfileResponse(
            UUID userId,
            String email,
            String displayName,
            String phone,
            String organization,
            String status
    ) {
    }

    private record MeResponse(
            ProfileResponse profile,
            List<MyHousehold> households
    ) {
    }

    private record MyHousehold(
            String houseId,
            String alias,
            String relation,
            String notifyPriority,
            boolean notifyEnabled
    ) {
    }
}

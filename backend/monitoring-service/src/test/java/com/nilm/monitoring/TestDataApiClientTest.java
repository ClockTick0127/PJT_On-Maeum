package com.nilm.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.client.ExpectedCount.once;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import com.nilm.monitoring.config.TestDataApiClient;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

class TestDataApiClientTest {

    private static final String BASE_URL = "http://device-service:8081";
    private static final UUID USER_ID = UUID.fromString(
            "11111111-1111-1111-1111-111111111111"
    );

    private MockRestServiceServer server;
    private TestDataApiClient client;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder();
        server = MockRestServiceServer.bindTo(builder).build();
        client = new TestDataApiClient(builder, BASE_URL);
    }

    @Test
    void signsUpLogsInAndReadsCanonicalUserSub() {
        expectSignup(HttpStatus.CREATED);
        expectLoginAndMe("[]");

        TestDataApiClient.SeedAccount account = client.ensureUser(
                "subject01@nilm.local",
                "Test1234!",
                "대상자 01",
                "01010000001",
                null
        );

        assertThat(account.userId()).isEqualTo(USER_ID);
        assertThat(account.accessToken()).isEqualTo("access-token");
        assertThat(account.householdRelations()).isEmpty();
        server.verify();
    }

    @Test
    void existingAccountIsRecoveredByLoginAndExistingHouseholdIsNotCreatedAgain() {
        expectSignup(HttpStatus.CONFLICT);
        expectLoginAndMe("""
                [{
                  "houseId": "H001",
                  "alias": "기존 가구",
                  "relation": "SELF",
                  "notifyPriority": "PRIMARY",
                  "notifyEnabled": true
                }]
                """);

        TestDataApiClient.SeedAccount account = client.ensureUser(
                "subject01@nilm.local",
                "Test1234!",
                "대상자 01",
                "01010000001",
                null
        );
        client.ensureHousehold(account, "H001", "테스트 가구 01", 30);

        assertThat(account.householdRelations()).containsEntry("H001", "SELF");
        server.verify();
    }

    @Test
    void createsMissingHouseholdWithBearerToken() {
        TestDataApiClient.SeedAccount account = new TestDataApiClient.SeedAccount(
                USER_ID,
                "access-token",
                Map.of()
        );
        server.expect(once(), requestTo(BASE_URL + "/api/devices/households"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(header("Authorization", "Bearer access-token"))
                .andRespond(withStatus(HttpStatus.CREATED));

        client.ensureHousehold(account, "H001", "테스트 가구 01", 30);

        server.verify();
    }

    @Test
    void existingNonSelfMembershipCannotBeUsedAsSubjectHousehold() {
        TestDataApiClient.SeedAccount account = new TestDataApiClient.SeedAccount(
                USER_ID,
                "access-token",
                Map.of("H001", "GUARDIAN")
        );

        assertThatThrownBy(() -> client.ensureHousehold(
                account, "H001", "테스트 가구 01", 30
        )).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SELF");
        server.verify();
    }

    @Test
    void householdOwnedByAnotherUserStopsInitialization() {
        TestDataApiClient.SeedAccount account = new TestDataApiClient.SeedAccount(
                USER_ID,
                "access-token",
                Map.of()
        );
        server.expect(once(), requestTo(BASE_URL + "/api/devices/households"))
                .andRespond(withStatus(HttpStatus.CONFLICT));

        assertThatThrownBy(() -> client.ensureHousehold(
                account, "H001", "테스트 가구 01", 30
        )).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("H001");
        server.verify();
    }

    private void expectSignup(HttpStatus status) {
        var response = status == HttpStatus.CREATED
                ? withSuccess("""
                        {
                          "userId": "%s",
                          "email": "subject01@nilm.local",
                          "displayName": "대상자 01",
                          "status": "ACTIVE"
                        }
                        """.formatted(USER_ID), MediaType.APPLICATION_JSON)
                : withStatus(status);
        server.expect(once(), requestTo(BASE_URL + "/api/auth/signup"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(response);
    }

    private void expectLoginAndMe(String householdsJson) {
        server.expect(once(), requestTo(BASE_URL + "/api/auth/login"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withSuccess("""
                        {
                          "accessToken": "access-token",
                          "refreshToken": "refresh-token",
                          "expiresIn": 300,
                          "tokenType": "Bearer"
                        }
                        """, MediaType.APPLICATION_JSON));
        server.expect(once(), requestTo(BASE_URL + "/api/auth/me"))
                .andExpect(method(HttpMethod.GET))
                .andExpect(header("Authorization", "Bearer access-token"))
                .andRespond(withSuccess("""
                        {
                          "profile": {
                            "userId": "%s",
                            "email": "subject01@nilm.local",
                            "displayName": "대상자 01",
                            "status": "ACTIVE"
                          },
                          "households": %s
                        }
                        """.formatted(USER_ID, householdsJson), MediaType.APPLICATION_JSON));
    }
}

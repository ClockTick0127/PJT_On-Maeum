package com.nilm.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.nilm.monitoring.config.ManagerInitializer;
import com.nilm.monitoring.config.TestDataApiClient;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

class ManagerInitializerTest {

    private JdbcTemplate jdbc;
    private TestDataApiClient api;

    @BeforeEach
    void setUp() {
        var dataSource = new DriverManagerDataSource(
                "jdbc:h2:mem:manager_seed_" + UUID.randomUUID()
                        + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
                "sa",
                ""
        );
        Flyway.configure().dataSource(dataSource).load().migrate();
        jdbc = new JdbcTemplate(dataSource);
        api = mock(TestDataApiClient.class);
        when(api.ensureUser(
                anyString(), anyString(), anyString(), anyString(), anyString()
        )).thenAnswer(invocation -> {
            String email = invocation.getArgument(0);
            return new TestDataApiClient.SeedAccount(
                    userId(email),
                    "access-token-" + email,
                    Map.of()
            );
        });
    }

    @Test
    void createsTenManagerAccountsAndLinksTheirSubs() {
        ManagerInitializer initializer = new ManagerInitializer(
                jdbc, api, "Test1234!"
        );
        initializer.afterSingletonsInstantiated();

        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*) FROM managers",
                Integer.class
        )).isEqualTo(10);
        assertThat(jdbc.queryForObject(
                "SELECT auth_sub FROM managers WHERE name = '담당자 01'",
                String.class
        )).isEqualTo(userId("manager01@nilm.local").toString());
        assertThat(jdbc.queryForObject(
                "SELECT phone FROM managers WHERE name = '담당자 01'",
                String.class
        )).isEqualTo("01020000001");
        assertThat(initializer.managerIdFor(10)).isPositive();
    }

    @Test
    void restartDoesNotDuplicateManagerRows() {
        new ManagerInitializer(jdbc, api, "Test1234!")
                .afterSingletonsInstantiated();
        new ManagerInitializer(jdbc, api, "Test1234!")
                .afterSingletonsInstantiated();

        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*) FROM managers",
                Integer.class
        )).isEqualTo(10);
    }

    private UUID userId(String email) {
        return UUID.nameUUIDFromBytes(email.getBytes(StandardCharsets.UTF_8));
    }
}

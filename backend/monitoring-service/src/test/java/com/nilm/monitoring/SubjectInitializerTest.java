package com.nilm.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.nilm.monitoring.config.ManagerInitializer;
import com.nilm.monitoring.config.SubjectInitializer;
import com.nilm.monitoring.config.TestDataApiClient;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

class SubjectInitializerTest {

    private JdbcTemplate jdbc;
    private TestDataApiClient api;
    private ManagerInitializer managers;

    @BeforeEach
    void setUp() {
        var dataSource = new DriverManagerDataSource(
                "jdbc:h2:mem:subject_seed_" + UUID.randomUUID()
                        + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
                "sa",
                ""
        );
        Flyway.configure().dataSource(dataSource).load().migrate();
        jdbc = new JdbcTemplate(dataSource);
        api = mock(TestDataApiClient.class);
        managers = mock(ManagerInitializer.class);
        doNothing().when(managers).initialize();

        for (int index = 1; index <= 10; index++) {
            jdbc.update("""
                    INSERT INTO managers(id, auth_sub, name, organization)
                    VALUES (?, ?, ?, ?)
                    """, (long) index, "manager-" + index,
                    "테스트 담당자 " + index, "테스트 기관");
            when(managers.managerIdFor(index)).thenReturn((long) index);
        }

        when(api.ensureUser(
                anyString(), anyString(), anyString(), anyString(), isNull()
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
    void createsThirtyHouseholdsPerManagerAndLinksSubjectsToUserSubs() {
        new SubjectInitializer(jdbc, api, managers, "Test1234!")
                .afterSingletonsInstantiated();

        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*) FROM subjects",
                Integer.class
        )).isEqualTo(300);
        assertThat(jdbc.queryForList(
                "SELECT manager_id, COUNT(*) AS cnt FROM subjects "
                        + "GROUP BY manager_id ORDER BY manager_id"
        )).hasSize(10)
                .allSatisfy(row -> assertThat(row.get("cnt"))
                        .asString().isEqualTo("30"));
        assertThat(managerOf("H030")).isEqualTo(1L);
        assertThat(managerOf("H031")).isEqualTo(2L);
        assertThat(managerOf("H300")).isEqualTo(10L);
        assertThat(jdbc.queryForObject(
                "SELECT auth_sub FROM subjects WHERE household_id = 'H300'",
                String.class
        )).isEqualTo(userId("subject300@nilm.local").toString());
        var first = jdbc.queryForMap(
                "SELECT * FROM subjects WHERE household_id = 'H001'"
        );
        assertThat(first.get("auth_sub"))
                .isEqualTo(userId("subject01@nilm.local").toString());
        assertThat(first.get("manager_id")).isEqualTo(1L);
        assertThat(first.get("monitoring_enabled")).isEqualTo(true);
    }

    @Test
    void restartDoesNotDuplicateRows() {
        new SubjectInitializer(jdbc, api, managers, "Test1234!")
                .afterSingletonsInstantiated();
        new SubjectInitializer(jdbc, api, managers, "Test1234!")
                .afterSingletonsInstantiated();

        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*) FROM subjects",
                Integer.class
        )).isEqualTo(300);
    }

    @Test
    void existingHouseholdWithoutSubIsLinkedInsteadOfDuplicated() {
        jdbc.update("""
                INSERT INTO subjects(household_id, birth_date, name, phone, address)
                VALUES ('H001', DATE '1950-01-01', '기존 대상자', '010', '기존 주소')
                """);

        new SubjectInitializer(jdbc, api, managers, "Test1234!")
                .afterSingletonsInstantiated();

        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*) FROM subjects WHERE household_id = 'H001'",
                Integer.class
        )).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "SELECT auth_sub FROM subjects WHERE household_id = 'H001'",
                String.class
        )).isEqualTo(userId("subject01@nilm.local").toString());
    }

    @Test
    void legacyHardCodedSubjectIsAdoptedByTheApiCreatedAccount() {
        jdbc.update("""
                INSERT INTO subjects(
                    household_id, auth_sub, birth_date, name, phone, address,
                    manager_memo
                ) VALUES (
                    'H001', 'test-subject-3', DATE '1950-01-01', '테스트 대상자',
                    '010-0000-0000', '테스트 주소', '로컬 자동 생성 테스트 데이터'
                )
                """);
        Long originalId = jdbc.queryForObject(
                "SELECT id FROM subjects WHERE household_id = 'H001'",
                Long.class
        );

        new SubjectInitializer(jdbc, api, managers, "Test1234!")
                .afterSingletonsInstantiated();

        var adopted = jdbc.queryForMap(
                "SELECT * FROM subjects WHERE household_id = 'H001'"
        );
        assertThat(adopted.get("id")).isEqualTo(originalId);
        assertThat(adopted.get("auth_sub"))
                .isEqualTo(userId("subject01@nilm.local").toString());
        assertThat(adopted.get("name")).isEqualTo("대상자 01");
        assertThat(adopted.get("manager_id")).isEqualTo(1L);
        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*) FROM subjects",
                Integer.class
        )).isEqualTo(300);
    }

    private Long managerOf(String houseId) {
        return jdbc.queryForObject(
                "SELECT manager_id FROM subjects WHERE household_id = ?",
                Long.class,
                houseId
        );
    }

    private UUID userId(String email) {
        return UUID.nameUUIDFromBytes(email.getBytes(StandardCharsets.UTF_8));
    }
}

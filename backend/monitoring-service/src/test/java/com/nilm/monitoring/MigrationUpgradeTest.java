package com.nilm.monitoring;
import java.sql.DriverManager;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class MigrationUpgradeTest {
    @Test
    void upgradesAnExistingV2DatabaseWithoutRepair() throws Exception {
        String url = "jdbc:h2:mem:migration_upgrade;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1";
        Flyway.configure().dataSource(url, "sa", "").target("2").load().migrate();
        var flyway = Flyway.configure().dataSource(url, "sa", "").load();
        assertThat(flyway.migrate().migrationsExecuted).isEqualTo(17); // V3~V19
        flyway.validate();
        try (var connection = DriverManager.getConnection(url, "sa", "");
             var statement = connection.createStatement();
             var result = statement.executeQuery("select count(*) from notifications")) {
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isZero();
        }
        try (var connection = DriverManager.getConnection(url, "sa", "");
             var statement = connection.createStatement();
             var result = statement.executeQuery("select count(*) from managers")) {
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isZero();
        }
        try (var connection = DriverManager.getConnection(url, "sa", "");
             var statement = connection.createStatement();
             var result = statement.executeQuery("select state_version, current_risk_score from subjects")) {
            assertThat(result.next()).isFalse();
        }
        for (String table : new String[]{
                "household_profiles",
                "household_routine_baselines",
                "household_profile_statistics",
                "household_observations",
                "household_daily_appliance_usage",
                "appliance_usage_episodes",
                "selected_scene_snapshots",
                "risk_assessments"}) {
            try (var connection = DriverManager.getConnection(url, "sa", "");
                 var statement = connection.createStatement();
                 var result = statement.executeQuery("select count(*) from " + table)) {
                assertThat(result.next()).isTrue();
                assertThat(result.getInt(1)).isZero();
            }
        }
    }
}

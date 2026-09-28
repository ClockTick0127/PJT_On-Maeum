package com.nilm.monitoring.config;

import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration(proxyBeanMethods = false)
public class DemoReportConfig {

    // Keep Boot's operational DataSource, JdbcTemplate and Flyway auto-configuration.
    @Bean(defaultCandidate = false)
    @Qualifier("demoReportDataSource")
    @ConfigurationProperties("app.demo-report.datasource")
    public HikariDataSource demoReportDataSource() {
        HikariDataSource source = new HikariDataSource();
        source.setPoolName("demo-report");
        source.setMaximumPoolSize(2);
        source.setMinimumIdle(0);
        source.setReadOnly(true);
        source.setConnectionTimeout(5000);
        source.setInitializationFailTimeout(-1);
        return source;
    }

    @Bean(defaultCandidate = false)
    @Qualifier("demoReportJdbcTemplate")
    public JdbcTemplate demoReportJdbcTemplate(
            @Qualifier("demoReportDataSource") DataSource source) {
        JdbcTemplate jdbc = new JdbcTemplate(source);
        jdbc.setQueryTimeout(15);
        return jdbc;
    }
}

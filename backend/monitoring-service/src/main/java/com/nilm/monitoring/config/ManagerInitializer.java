package com.nilm.monitoring.config;

import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/** Creates ten test manager accounts through the signup API and links their subs locally. */
@Slf4j
@Component
@DependsOnDatabaseInitialization
@ConditionalOnProperty(name = "app.test-data.enabled", havingValue = "true")
public class ManagerInitializer implements SmartInitializingSingleton {

    static final int ACCOUNT_COUNT = 10;
    static final String ORGANIZATION = "NILM 테스트 복지기관";

    private final JdbcTemplate jdbc;
    private final TestDataApiClient api;
    private final String password;
    private final Map<Integer, Long> managerIds = new HashMap<>();
    private boolean initialized;

    public ManagerInitializer(
            JdbcTemplate jdbc,
            TestDataApiClient api,
            @Value("${app.test-data.password:}") String password
    ) {
        this.jdbc = jdbc;
        this.api = api;
        this.password = password;
    }

    @Override
    public void afterSingletonsInstantiated() {
        initialize();
    }

    public synchronized void initialize() {
        if (initialized) {
            return;
        }
        requirePassword();

        for (int index = 1; index <= ACCOUNT_COUNT; index++) {
            String name = String.format("담당자 %02d", index);
            String email = String.format("manager%02d@nilm.local", index);
            String phone = String.format("0102%07d", index);
            TestDataApiClient.SeedAccount account = api.ensureUser(
                    email, password, name, phone, ORGANIZATION
            );

            jdbc.update("""
                    INSERT INTO managers (auth_sub, name, organization, phone, email)
                    SELECT ?, ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM managers WHERE auth_sub = ?
                    )
                    """, account.userId().toString(), name, ORGANIZATION, phone, email,
                    account.userId().toString());

            jdbc.update("""
                    UPDATE managers
                    SET phone = COALESCE(phone, ?),
                        email = COALESCE(email, ?)
                    WHERE auth_sub = ?
                    """, phone, email, account.userId().toString());

            Long managerId = jdbc.queryForObject(
                    "SELECT id FROM managers WHERE auth_sub = ?",
                    Long.class,
                    account.userId().toString()
            );
            managerIds.put(index, managerId);
        }

        initialized = true;
        log.info("테스트 담당자 {}명의 계정과 managers 연결 완료", ACCOUNT_COUNT);
    }

    public synchronized long managerIdFor(int index) {
        initialize();
        Long managerId = managerIds.get(index);
        if (managerId == null) {
            throw new IllegalArgumentException("테스트 담당자 순번이 범위를 벗어났습니다: " + index);
        }
        return managerId;
    }

    private void requirePassword() {
        if (!StringUtils.hasText(password)) {
            throw new IllegalStateException(
                    "TEST_DATA_ENABLED=true일 때 TEST_ACCOUNT_PASSWORD가 필요합니다."
            );
        }
    }
}

package com.nilm.monitoring;

import static org.assertj.core.api.Assertions.assertThat;

import com.nilm.monitoring.dto.kafka.ManagerRegisteredMessage;
import com.nilm.monitoring.repository.ManagerRepository;
import com.nilm.monitoring.service.ManagerRegistrationService;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 가입한 복지사가 담당자 명단에 오르는 구간.
 * 발행은 at-least-once라 같은 메시지가 두 번 와도 한 명이어야 한다.
 */
@SpringBootTest
class ManagerRegistrationFlowTest {

    private static final String AUTH_SUB = "keycloak-sub-1";

    @Autowired ManagerRegistrationService service;
    @Autowired ManagerRepository managers;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void setup() {
        jdbc.update("delete from notification_settings");
        jdbc.update("delete from subjects");
        jdbc.update("delete from managers");
    }

    private ManagerRegisteredMessage message(String authSub, String email) {
        return new ManagerRegisteredMessage(
                UUID.randomUUID(), authSub, "홍길동", "행복복지관",
                "01012345678", email, OffsetDateTime.now());
    }

    @Test
    void 가입한_복지사가_담당자로_등록된다() {
        service.receive(message(AUTH_SUB, "welfare@nilm.local"));

        var manager = managers.findByAuthSub(AUTH_SUB).orElseThrow();
        assertThat(manager.getName()).isEqualTo("홍길동");
        assertThat(manager.getOrganization()).isEqualTo("행복복지관");
        assertThat(manager.getPhone()).isEqualTo("01012345678");
        assertThat(manager.getEmail()).isEqualTo("welfare@nilm.local");
    }

    @Test
    void 같은_가입이_두_번_와도_담당자는_하나다() {
        service.receive(message(AUTH_SUB, "welfare@nilm.local"));
        service.receive(message(AUTH_SUB, "welfare@nilm.local"));

        assertThat(managers.findAll()).hasSize(1);
    }

    @Test
    void 프로필_수정을_재전송이_되돌리지_않는다() {
        service.receive(message(AUTH_SUB, "welfare@nilm.local"));
        var manager = managers.findByAuthSub(AUTH_SUB).orElseThrow();
        jdbc.update("update managers set organization = ? where id = ?", "이사한복지관", manager.getId());

        service.receive(message(AUTH_SUB, "welfare@nilm.local"));

        assertThat(managers.findByAuthSub(AUTH_SUB).orElseThrow().getOrganization())
                .isEqualTo("이사한복지관");
    }

    @Test
    void 이메일이_겹치면_그_값만_비우고_등록한다() {
        service.receive(message(AUTH_SUB, "welfare@nilm.local"));

        service.receive(message("keycloak-sub-2", "welfare@nilm.local"));

        var second = managers.findByAuthSub("keycloak-sub-2").orElseThrow();
        assertThat(second.getEmail()).isNull();
        assertThat(second.getOrganization()).isEqualTo("행복복지관");
    }

    @Test
    void authSub이_없는_메시지는_버린다() {
        service.receive(new ManagerRegisteredMessage(
                UUID.randomUUID(), null, "홍길동", "행복복지관",
                null, null, OffsetDateTime.now()));

        assertThat(managers.findAll()).isEmpty();
    }

    @Test
    void 소속이_비면_담당자로_만들지_않는다() {
        service.receive(new ManagerRegisteredMessage(
                UUID.randomUUID(), AUTH_SUB, "홍길동", "  ",
                null, null, OffsetDateTime.now()));

        assertThat(managers.findAll()).isEmpty();
    }
}

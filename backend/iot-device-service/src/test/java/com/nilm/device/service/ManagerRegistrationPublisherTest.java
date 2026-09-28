package com.nilm.device.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.nilm.device.domain.ManagerRegistrationOutbox;
import com.nilm.device.domain.UserProfile;
import com.nilm.device.repository.ManagerRegistrationOutboxRepository;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 브로커가 없는 상태에서 릴레이를 돌린다. 발행은 실패하지만 가입이 사라져서는 안 되고,
 * 다음 주기에 다시 나갈 수 있게 PENDING으로 남아 있어야 한다.
 */
@SpringBootTest(properties = {
        "app.manager-registration.publisher.enabled=true",
        // 스케줄러가 끼어들면 단정이 흔들린다. 테스트가 직접 부른다.
        "app.manager-registration.publisher.interval-ms=3600000",
        "app.manager-registration.publisher.send-timeout=1s",
        "app.manager-registration.publisher.retry-after=30s"
})
class ManagerRegistrationPublisherTest {

    @Autowired ManagerRegistrationPublisher publisher;
    @Autowired ManagerRegistrationOutboxRepository outbox;

    @BeforeEach
    void setup() {
        outbox.deleteAll();
        outbox.save(new ManagerRegistrationOutbox(
                new UserProfile(UUID.randomUUID(), "welfare@nilm.local", "홍길동",
                        "01012345678", "행복복지관"),
                OffsetDateTime.now()));
    }

    @Test
    void 발행에_실패해도_가입은_남아_다시_시도된다() {
        publisher.publishPending();

        ManagerRegistrationOutbox row = outbox.findAll().get(0);
        assertThat(row.getStatus()).isEqualTo(ManagerRegistrationOutbox.Status.PENDING);
        assertThat(row.getAttemptCount()).isEqualTo(1);
        assertThat(row.getLastError()).isNotBlank();
        assertThat(row.getPublishedAt()).isNull();
        // 브로커가 죽어 있는 동안 매 주기마다 매달리지 않도록 뒤로 미룬다.
        assertThat(row.getNextAttemptAt()).isAfter(OffsetDateTime.now());
    }

    @Test
    void 아직_때가_되지_않은_행은_건드리지_않는다() {
        publisher.publishPending();
        publisher.publishPending();

        // 첫 시도로 next_attempt_at이 밀렸으므로 두 번째 호출은 아무 일도 하지 않는다.
        assertThat(outbox.findAll().get(0).getAttemptCount()).isEqualTo(1);
    }
}

package com.nilm.monitoring;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nilm.monitoring.api.WebPushTestController;
import com.nilm.monitoring.repository.NotificationRepository;
import com.nilm.monitoring.repository.PushSubscriptionRepository;
import com.nilm.monitoring.service.*;
import nl.martijndwars.webpush.PushService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class PushFeatureConfigurationTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(WebPushSender.class, NotificationDispatchService.class,
                    NotificationDispatchListener.class, WebPushTestService.class, WebPushTestController.class)
            .withBean(PushService.class, () -> mock(PushService.class))
            .withBean(ObjectMapper.class, ObjectMapper::new)
            .withBean(NotificationRepository.class, () -> mock(NotificationRepository.class))
            .withBean(PushSubscriptionRepository.class, () -> mock(PushSubscriptionRepository.class))
            .withBean(NotificationService.class, () -> mock(NotificationService.class));

    @Test
    void localEnabledRegistersTestEndpoint() {
        runner.withPropertyValues("spring.profiles.active=local", "app.push.enabled=true")
                .run(context -> {
                    assertThat(context).hasSingleBean(WebPushTestController.class);
                    assertThat(context).hasSingleBean(NotificationDispatchListener.class);
                });
    }

    @Test
    void disabledPushDoesNotRequireSenderOrTestEndpoint() {
        runner.withPropertyValues("spring.profiles.active=local", "app.push.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(WebPushSender.class);
                    assertThat(context).doesNotHaveBean(WebPushTestController.class);
                    assertThat(context).doesNotHaveBean(NotificationDispatchListener.class);
                });
    }

    @Test
    void nonLocalDoesNotExposeTestEndpoint() {
        runner.withPropertyValues("app.push.enabled=true")
                .run(context -> assertThat(context).doesNotHaveBean(WebPushTestController.class));
    }
}


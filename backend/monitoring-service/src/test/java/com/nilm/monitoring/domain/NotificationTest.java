package com.nilm.monitoring.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class NotificationTest {

    @Test
    void 생성_시_created_at과_updated_at이_같은_시각으로_채워진다() {
        OffsetDateTime before = OffsetDateTime.now(ZoneOffset.UTC);
        Notification notification = new Notification(UUID.randomUUID(), "manager-1");

        assertThat(notification.getCreatedAt()).isNotNull();
        assertThat(notification.getUpdatedAt()).isEqualTo(notification.getCreatedAt());
        assertThat(notification.getCreatedAt()).isAfterOrEqualTo(before);
    }

    @Test
    void 상태_변경은_updated_at만_갱신하고_created_at은_그대로_둔다() {
        Notification notification = new Notification(UUID.randomUUID(), "manager-1");
        OffsetDateTime createdAt = notification.getCreatedAt();
        OffsetDateTime later = createdAt.plusMinutes(5);

        notification.changeManagerStatus(Notification.ManagerResponseStatus.ACKNOWLEDGED, later);

        assertThat(notification.getCreatedAt()).isEqualTo(createdAt);
        assertThat(notification.getUpdatedAt()).isEqualTo(later);
    }

    @Test
    void 응답과_만료도_updated_at을_갱신한다() {
        Notification notification = new Notification(UUID.randomUUID(), "manager-1");
        OffsetDateTime createdAt = notification.getCreatedAt();
        notification.requestResponse(createdAt.plusMinutes(30));

        OffsetDateTime answeredAt = createdAt.plusMinutes(10);
        notification.answer(true, answeredAt);
        assertThat(notification.getUpdatedAt()).isEqualTo(answeredAt);

        Notification overdue = new Notification(UUID.randomUUID(), "manager-1");
        OffsetDateTime deadline = overdue.getCreatedAt().plusMinutes(30);
        overdue.requestResponse(deadline);
        overdue.expireIfOverdue(deadline.plusSeconds(1));
        assertThat(overdue.getResponseStatus()).isEqualTo(Notification.ResponseStatus.EXPIRED);
        assertThat(overdue.getUpdatedAt()).isEqualTo(deadline.plusSeconds(1));
    }

    @Test
    void 발송_결과_기록도_updated_at을_갱신한다() {
        Notification notification = new Notification(UUID.randomUUID(), "manager-1");
        OffsetDateTime createdAt = notification.getCreatedAt();

        notification.markSent();

        assertThat(notification.getUpdatedAt()).isAfterOrEqualTo(createdAt);
        assertThat(notification.getCreatedAt()).isEqualTo(createdAt);
    }

    @Test
    void 과거_시각으로_상태를_바꿔도_updated_at은_뒤로_가지_않는다() {
        Notification notification = new Notification(UUID.randomUUID(), "manager-1");
        OffsetDateTime createdAt = notification.getCreatedAt();

        notification.changeManagerStatus(
                Notification.ManagerResponseStatus.ACKNOWLEDGED, createdAt.minusHours(1));

        assertThat(notification.getManagerStatusUpdatedAt()).isEqualTo(createdAt.minusHours(1));
        assertThat(notification.getUpdatedAt()).isEqualTo(createdAt);
    }
}

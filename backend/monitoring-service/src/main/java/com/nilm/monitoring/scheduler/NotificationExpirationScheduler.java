package com.nilm.monitoring.scheduler;

import com.nilm.monitoring.service.NotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
/**
 * 응답없는 알림을 만료시키는 스케줄러
 * */
@Component
@RequiredArgsConstructor
public class NotificationExpirationScheduler {

    private final NotificationService service;

    @Scheduled(fixedDelay = 5000)
    public void expireNotifications() {
        service.expireOverdue();
    }
}

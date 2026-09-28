package com.nilm.monitoring.scheduler;

import com.nilm.monitoring.service.AwayModeService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 예약된 외출의 시작·종료를 발효시키는 스케줄러.
 *
 * <p>위험 이벤트 처리 경로는 스케줄러를 기다리지 않고 직접 구간을 계산하므로,
 * 여기서 늦어져도 알림 판정은 어긋나지 않는다. 이 주기는 담당자 대시보드에
 * 외출 상태가 반영되기까지의 지연이다.
 */
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(
        name = "app.away-mode.scheduler-enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class AwayModeScheduler {

    private final AwayModeService service;

    @Scheduled(fixedDelayString = "${app.away-mode.scheduler-interval-ms:30000}")
    public void applyScheduledTransitions() {
        service.applyScheduledTransitions();
    }
}

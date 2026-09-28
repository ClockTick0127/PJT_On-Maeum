package com.nilm.monitoring.scheduler;

import com.nilm.monitoring.service.SubjectStatusStreamRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 이상 징후가 한동안 없어도 SSE 연결이 유지되도록 주기적으로 주석을 보낸다.
 * 게이트웨이와 Nginx의 유휴 타임아웃보다 짧은 주기여야 한다.
 */
@Component
@RequiredArgsConstructor
public class SubjectStatusStreamHeartbeatScheduler {

    private final SubjectStatusStreamRegistry registry;

    @Scheduled(fixedDelay = 25_000)
    public void sendHeartbeat() {
        registry.heartbeat();
    }
}

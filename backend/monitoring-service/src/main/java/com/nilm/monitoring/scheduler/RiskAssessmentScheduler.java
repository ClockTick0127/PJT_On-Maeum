package com.nilm.monitoring.scheduler;

import com.nilm.monitoring.config.enums.StateChangeTrigger;
import com.nilm.monitoring.service.RiskAssessmentService;
import com.nilm.monitoring.service.RiskAssessmentService.EvaluationTarget;
import java.time.Clock;
import java.time.OffsetDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 주기적으로 도는 위험 평가.
 *
 * <p>상태 변경만으로는 부족하다. 무활동과 루틴 미사용은 "아무 일도 일어나지 않는 것"이
 * 곧 신호이므로, 이벤트가 오지 않는 동안에도 누군가는 시계를 보고 있어야 한다.
 *
 * <p>가구마다 트랜잭션을 따로 연다. 한 가구의 실패가 나머지 가구의 평가를 막으면
 * 정작 위험한 가구를 놓칠 수 있다.
 *
 * <p>잠금 범위도 가구다. 한 가구를 평가하는 동안 다른 가구는 기다리지 않는다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(
        name = "app.risk.scheduler-enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class RiskAssessmentScheduler {

    private final RiskAssessmentService service;

    /** 평가 기준 시각의 출처. 테스트는 고정 시계를 넣어 유지시간·재발송 창 판정을 재현한다. */
    private final Clock clock;

    /**
     * 기동 직후 첫 실행은 잠시 미룰 수 있다. 초기 지연이 없으면 컨텍스트가 뜨는 순간 배경에서
     * 한 바퀴 돌기 시작해, 스케줄러를 직접 부르는 테스트와 같은 표를 놓고 경쟁한다.
     */
    @Scheduled(
            fixedDelayString = "${app.risk.scheduler-interval-ms:60000}",
            initialDelayString = "${app.risk.scheduler-initial-delay-ms:0}"
    )
    public void evaluateAll() {
        OffsetDateTime now = OffsetDateTime.now(clock);

        // 목록은 식별자만 읽는다. 여기서 대상자를 통째로 읽어 두면 가구 트랜잭션이
        // 잠금을 잡기 전 상태를 들고 시작하게 된다.
        for (EvaluationTarget target : service.targets()) {
            try {
                service.evaluateSubject(target, now, StateChangeTrigger.ASSESSMENT);
            } catch (Exception e) {
                log.error("대상자 위험 평가 실패: subjectId={}, householdId={}",
                        target.subjectId(), target.householdId(), e);
            }
        }
    }
}

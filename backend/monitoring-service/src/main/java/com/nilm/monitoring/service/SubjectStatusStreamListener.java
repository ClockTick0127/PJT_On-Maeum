package com.nilm.monitoring.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * 커밋된 상태 변경만 화면으로 내보낸다.
 * 롤백된 트랜잭션의 값이 대시보드에 잠깐이라도 보이면 안 된다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SubjectStatusStreamListener {

    private final SubjectStatusStreamService stream;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onChanged(SubjectStateChanged event) {
        try {
            stream.publish(event.subjectId(), event.trigger());
        } catch (Exception e) {
            // 커밋은 이미 끝났다. 화면 갱신 실패를 Kafka·API 처리 실패로 위장하지 않는다.
            log.error("실시간 상태 전송 실패: subjectId={}", event.subjectId(), e);
        }
    }
}

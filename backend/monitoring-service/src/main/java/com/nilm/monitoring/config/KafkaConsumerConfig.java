package com.nilm.monitoring.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Kafka 리스너의 공통 에러 처리.
 *
 * <p>예전에는 {@code CommonContainerStoppingErrorHandler}로 첫 예외에서 컨테이너를 멈췼다.
 * 대상자가 등록되지 않은 가구의 이벤트 한 건이 들어오면 그 뒤로 모든 가구의 알림이
 * 재시작 전까지 끊겼고, 그 사실은 로그 한 줄 외에 어디에도 드러나지 않았다.
 *
 * <p>이제 레코드 단위로 처리한다. 일시적 장애(DB 락, 커넥션)는 짧게 재시도하고,
 * 재시도해도 결과가 바뀌지 않는 예외(잘못된 페이로드, 상태 위반)는 즉시 건너뛴다.
 * 건너뛴 레코드는 원문을 포함해 ERROR로 남겨 수동 재처리가 가능하게 한다.
 * 리스너가 살아 있는지는 {@link KafkaListenerHealthIndicator}가 readiness에 노출한다.
 */
@Configuration
@Slf4j
public class KafkaConsumerConfig {

    /** 일시적 장애 재시도 간격. */
    static final long RETRY_INTERVAL_MS = 1_000L;

    /** 첫 시도 이후 추가 재시도 횟수. 총 시도 횟수는 이 값 + 1이다. */
    static final long MAX_RETRIES = 2L;

    @Bean
    public CommonErrorHandler kafkaErrorHandler() {
        return kafkaErrorHandler(new LoggingSkipRecoverer());
    }

    /** 복구 동작을 바꿔 끼울 수 있게 분리한다. 테스트는 여기로 spy를 넣는다. */
    static DefaultErrorHandler kafkaErrorHandler(ConsumerRecordRecoverer recoverer) {
        DefaultErrorHandler handler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRIES)
        );
        // 다시 읽어도 같은 결과가 나오는 예외는 재시도하지 않는다.
        // JsonProcessingException: 역직렬화 불가 페이로드
        // IllegalArgumentException: 필수 필드 누락 등 계약 위반
        // IllegalStateException: 가구·대상자 등록 상태 위반
        handler.addNotRetryableExceptions(
                JsonProcessingException.class,
                IllegalArgumentException.class,
                IllegalStateException.class
        );
        return handler;
    }

    /**
     * 재시도가 끝난 레코드를 건너뛰며 원문을 남긴다.
     *
     * <p>분석 이벤트 페이로드는 가구 ID와 판정 근거만 담고 있어 로그에 남겨도 된다.
     * 재처리가 필요하면 이 로그의 값을 그대로 다시 발행한다.
     */
    static final class LoggingSkipRecoverer implements ConsumerRecordRecoverer {
        @Override
        public void accept(ConsumerRecord<?, ?> record, Exception exception) {
            Throwable cause = exception.getCause() != null ? exception.getCause() : exception;
            log.error(
                    "Kafka 레코드 처리 실패로 건너뜀: topic={}, partition={}, offset={}, key={}, "
                            + "errorType={}, error={}, value={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    cause.getClass().getSimpleName(),
                    cause.getMessage(),
                    record.value()
            );
        }
    }
}

package com.nilm.monitoring.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * 에러 핸들러는 레코드를 건너뛰되 컨테이너는 멈추지 않아야 한다.
 *
 * <p>예전 {@code CommonContainerStoppingErrorHandler}는 대상자 미등록 가구 이벤트 한 건으로
 * 모든 가구의 알림을 끊었다. 이 테스트는 그 회귀를 막는다.
 */
class KafkaConsumerConfigTest {

    private ConsumerRecordRecoverer recoverer;
    private DefaultErrorHandler handler;
    private Consumer<?, ?> consumer;
    private MessageListenerContainer container;

    @BeforeEach
    void setUp() {
        recoverer = mock(ConsumerRecordRecoverer.class);
        handler = KafkaConsumerConfig.kafkaErrorHandler(recoverer);
        consumer = mock(Consumer.class);
        // 멈춰 있는 컨테이너로 두면 재시도 대기(stoppableSleep)가 즉시 끝나 테스트가 빠르다.
        container = mock(MessageListenerContainer.class);
    }

    private static ConsumerRecord<?, ?> record(long offset) {
        return new ConsumerRecord<>("analysis.event.v1", 0, offset, "H006", "{\"household_id\":\"H006\"}");
    }

    @Test
    void 기본_빈은_컨테이너를_멈추지_않는_DefaultErrorHandler다() {
        assertThat(new KafkaConsumerConfig().kafkaErrorHandler())
                .isInstanceOf(DefaultErrorHandler.class);
    }

    @Test
    void 상태_위반_예외는_재시도_없이_즉시_건너뛴다() {
        ConsumerRecord<?, ?> record = record(7L);
        Exception failure = new ListenerExecutionFailedException(
                "listener failed",
                new IllegalStateException("가구에 정확히 한 명의 대상자를 등록해야 합니다: H006"));

        handler.handleRemaining(failure, List.of(record), consumer, container);

        verify(recoverer, times(1)).accept(eq(record), any(Exception.class));
    }

    @Test
    void 계약_위반과_역직렬화_실패도_즉시_건너뛴다() {
        ConsumerRecord<?, ?> invalid = record(8L);
        handler.handleRemaining(
                new ListenerExecutionFailedException("listener failed",
                        new IllegalArgumentException("분석 이벤트 필수 필드가 유효하지 않습니다.")),
                List.of(invalid), consumer, container);
        verify(recoverer, times(1)).accept(eq(invalid), any(Exception.class));

        ConsumerRecord<?, ?> malformed = record(9L);
        handler.handleRemaining(
                new ListenerExecutionFailedException("listener failed",
                        new com.fasterxml.jackson.core.JsonParseException(null, "bad json")),
                List.of(malformed), consumer, container);
        verify(recoverer, times(1)).accept(eq(malformed), any(Exception.class));
    }

    @Test
    void 일시적_장애는_재시도_후에만_건너뛴다() {
        ConsumerRecord<?, ?> record = record(10L);
        Exception transientFailure = new ListenerExecutionFailedException(
                "listener failed", new CannotAcquireLockException("lock timeout"));

        // 첫 전달이 실패하면 MAX_RETRIES 번 재시도한다. 재시도 동안은 건너뛰지 않고
        // 같은 오프셋으로 되돌리며, 핸들러는 컨테이너에게 "재시도 중"을 알리는
        // RecordInRetryException을 던진다. 컨테이너가 이를 받아 다음 poll에서 다시 전달한다.
        for (int retry = 0; retry < KafkaConsumerConfig.MAX_RETRIES; retry++) {
            assertThatThrownBy(() ->
                    handler.handleRemaining(transientFailure, List.of(record), consumer, container))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Record in retry and not yet recovered");
            verify(recoverer, never()).accept(any(), any());
        }

        // 재시도가 소진된 다음 전달(총 MAX_RETRIES + 1번째)은 예외 없이 복구(건너뛰기)로 끝난다.
        handler.handleRemaining(transientFailure, List.of(record), consumer, container);
        verify(recoverer, times(1)).accept(eq(record), any(Exception.class));
    }
}

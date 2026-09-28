package com.nilm.monitoring.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 시각을 읽는 단일 출처.
 *
 * <p>타이머 평가처럼 "지금"을 기준으로 판단하는 코드가 {@code OffsetDateTime.now()}를
 * 직접 부르면 테스트가 시각을 고정할 수 없어 실행 시점에 따라 결과가 갈린다.
 * {@link Clock}을 주입받으면 운영은 시스템 시계를, 테스트는 고정 시계를 쓴다.
 */
@Configuration
public class ClockConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}

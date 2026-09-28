package com.nilm.gateway.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpMethod;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

/**
 * Gateway 1차 JWT 검증. 유효한 토큰인지만 확인하고,
 * 역할(ADMIN 등) 인가는 각 도메인 서비스가 담당한다.
 */
@Configuration
@EnableWebFluxSecurity
public class SecurityConfig {

    @Bean
    @ConditionalOnProperty(name = "app.security.enabled", havingValue = "false", matchIfMissing = true)
    public SecurityWebFilterChain permitAllFilterChain(ServerHttpSecurity http) {
        http
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .authorizeExchange(exchange -> exchange.anyExchange().permitAll());
        return http.build();
    }

    @Bean
    @ConditionalOnProperty(name = "app.security.enabled", havingValue = "true")
    public SecurityWebFilterChain securedFilterChain(ServerHttpSecurity http) {
        http
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .authorizeExchange(exchange -> exchange
                        .pathMatchers("/actuator/health/**", "/actuator/info").permitAll()
                        // 가입·로그인·토큰 갱신은 토큰이 없는 상태에서 호출된다
                        .pathMatchers("/api/auth/signup", "/api/auth/login",
                                "/api/auth/refresh",
                                // 가입 폼의 중복 확인 — 아직 계정이 없는 단계다
                                "/api/auth/check-email",
                                // 만료된 access token으로도 로그아웃은 되어야 한다.
                                // 폐기 대상은 본문의 refresh token이라 남의 세션은 끊을 수 없다.
                                "/api/auth/logout").permitAll()
                        // 알림 응답은 일회용 토큰으로 인증한다. 어르신에게 위급 상황에
                        // 로그인을 요구하면 응답률이 떨어지고, 무응답으로 처리되어
                        // 담당자가 헛걸음하게 된다. 토큰으로 할 수 있는 일은
                        // 해당 알림에 예/아니오를 기록하는 것 하나뿐이다.
                        .pathMatchers(HttpMethod.POST,
                                "/api/monitoring/notifications/respond").permitAll()
                        .anyExchange().authenticated())
                .oauth2ResourceServer(oauth2 -> oauth2.jwt(Customizer.withDefaults()));
        return http.build();
    }
}
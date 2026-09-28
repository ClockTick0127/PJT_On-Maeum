package com.nilm.monitoring.config;

import com.nilm.monitoring.config.enums.RiskLevel;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 분석 서비스 이벤트를 유형별로 어떻게 다룰지 정한 표(설계 11.4절).
 *
 * <p>모든 이벤트를 하나의 흐름으로 처리하면 판단 주체가 섞인다.
 * 장시간 무활동·장시간 사용은 분석 서비스가 소유하는 판단이라 즉시 알림으로 내보낸다.
 * 모니터링 자체 평가는 이 판단들을 점수화하지 않는다. 판단에 쓰지 않을 유형(루틴 미사용)은
 * SHADOW로 두어 저장·화면 표시만 하고, 근거로만 남길 유형은 EVIDENCE로 둔다.
 *
 * <p>되돌리기는 설정 한 줄을 바꾸는 것으로 끝나야 한다. 그래서 유형 표를 코드가 아니라
 * {@code app.risk.event-routing.*}에 둔다. 모르는 유형은 기본값으로 처리해,
 * 분석 서비스가 새 유형을 추가해도 소비자가 멈추지 않게 한다.
 */
@Slf4j
@Component
public class EventRoutingPolicy {

    /** 유형 표가 아니라 정책 자체를 가리키는 키들. */
    private static final String KEY_DEFAULT = "DEFAULT";
    private static final String KEY_SUPPRESS_WHILE_AWAY = "SUPPRESS_WHILE_AWAY";
    /** 기기가 꺼져 있던 구간의 이벤트를 억제할 유형 */
    private static final String KEY_SUPPRESS_WHILE_DEVICE_OFFLINE = "SUPPRESS_WHILE_DEVICE_OFFLINE";
    /** 예전에 응답 요구 유형을 고르던 키. 지금은 모든 알림이 응답을 받으므로 무시한다. */
    private static final String KEY_RESPONSE_REQUIRED = "RESPONSE_REQUIRED";
    private static final String KEY_RESPONSE_DEADLINE_SECONDS = "RESPONSE_DEADLINE_SECONDS";

    private static final Route FALLBACK = new Route(Mode.EVIDENCE, null);

    /** 이벤트 한 건을 어디까지 반영할지. */
    public enum Mode {

        /** 즉시 해당 등급으로 알림까지 낸다. */
        IMMEDIATE,

        /** 저장과 화면 표시만. 등급·알림 없이 자체 평가와 비교하는 기간에 쓴다. */
        SHADOW,

        /** 저장만. 보고서 근거로 남긴다. */
        EVIDENCE
    }

    /**
     * @param level IMMEDIATE일 때 세울 등급. 다른 모드에서는 null
     */
    public record Route(Mode mode, RiskLevel level) {
    }

    private final Map<String, Route> routes;
    private final Route defaultRoute;
    private final Set<String> suppressWhileAway;
    private final Set<String> suppressWhileDeviceOffline;
    private final Duration responseDeadline;

    public EventRoutingPolicy(RiskProperties properties) {
        Map<String, Route> parsed = new java.util.LinkedHashMap<>();
        Route configuredDefault = FALLBACK;
        Set<String> away = Set.of();
        Set<String> deviceOffline = Set.of();
        Duration deadline = Duration.ofSeconds(60);

        for (Map.Entry<String, String> entry : properties.getEventRouting().entrySet()) {
            String key = normalize(entry.getKey());
            String value = entry.getValue() == null ? "" : entry.getValue().trim();
            switch (key) {
                case KEY_DEFAULT -> configuredDefault = parseRoute(key, value);
                case KEY_SUPPRESS_WHILE_AWAY -> away = parseSet(value);
                case KEY_SUPPRESS_WHILE_DEVICE_OFFLINE -> deviceOffline = parseSet(value);
                case KEY_RESPONSE_REQUIRED -> log.warn(
                        "app.risk.event-routing.response-required는 더 이상 쓰지 않는다. "
                                + "모든 알림이 응답을 받는다: {}", value);
                case KEY_RESPONSE_DEADLINE_SECONDS -> deadline = parseDeadline(value, deadline);
                default -> parsed.put(key, parseRoute(key, value));
            }
        }

        this.routes = Map.copyOf(parsed);
        this.defaultRoute = configuredDefault;
        this.suppressWhileAway = away;
        this.suppressWhileDeviceOffline = deviceOffline;
        this.responseDeadline = deadline;
    }

    /** 알 수 없는 유형은 기본값으로 처리한다. */
    public Route routeOf(String eventType) {
        if (eventType == null || eventType.isBlank()) {
            return defaultRoute;
        }
        return routes.getOrDefault(normalize(eventType), defaultRoute);
    }

    /**
     * 외출 중에 알림을 억제할 유형인지.
     *
     * <p>장시간 사용은 부재 중일수록 위험하므로 이 목록에 넣지 않는다.
     * "외출 중이면 알림 생략"을 모든 유형에 일괄 적용하지 않는다는 것이 이 표의 요점이다.
     */
    public boolean suppressWhileAway(String eventType) {
        return eventType != null && suppressWhileAway.contains(normalize(eventType));
    }

    /**
     * 기기가 꺼져 있던 구간이면 억제할 유형인지.
     *
     * <p>무활동은 "사람이 아무것도 안 썼다"는 뜻인데 기기가 꺼져 있었다면
     * 사람에 대한 신호가 아니다. 반대로 장시간 가전 사용처럼 이미 켜져 있던
     * 사실에 근거한 유형은 억제 대상이 아니다.
     */
    public boolean suppressWhileDeviceOffline(String eventType) {
        return eventType != null && suppressWhileDeviceOffline.contains(normalize(eventType));
    }

    public Duration responseDeadline() {
        return responseDeadline;
    }

    /**
     * 설정 키의 표기 차이를 흡수한다.
     * 바인딩 과정에서 대소문자나 구분자가 달라져도 같은 유형으로 읽히게 한다.
     */
    private static String normalize(String value) {
        return value.trim().toUpperCase(Locale.ROOT).replace('-', '_').replace('.', '_');
    }

    /** {@code IMMEDIATE:DANGER} 또는 {@code SHADOW} 형태를 읽는다. */
    private Route parseRoute(String key, String value) {
        if (value.isBlank()) {
            log.warn("이벤트 라우팅 값이 비어 있어 기본값으로 둔다: key={}", key);
            return FALLBACK;
        }
        String[] parts = value.split(":", 2);
        Mode mode;
        try {
            mode = Mode.valueOf(normalize(parts[0]));
        } catch (IllegalArgumentException e) {
            log.warn("알 수 없는 이벤트 라우팅 모드라 저장만 한다: key={}, value={}", key, value);
            return FALLBACK;
        }

        if (mode != Mode.IMMEDIATE) {
            return new Route(mode, null);
        }
        if (parts.length < 2) {
            log.warn("즉시 알림에 등급이 없어 저장만 한다: key={}, value={}", key, value);
            return FALLBACK;
        }
        try {
            return new Route(Mode.IMMEDIATE, RiskLevel.valueOf(normalize(parts[1])));
        } catch (IllegalArgumentException e) {
            log.warn("알 수 없는 등급이라 저장만 한다: key={}, value={}", key, value);
            return FALLBACK;
        }
    }

    private static Set<String> parseSet(String value) {
        if (value.isBlank()) {
            return Set.of();
        }
        return Set.copyOf(new HashSet<>(Arrays.stream(value.split(","))
                .map(EventRoutingPolicy::normalize)
                .filter(item -> !item.isBlank())
                .toList()));
    }

    private Duration parseDeadline(String value, Duration fallback) {
        try {
            return Duration.ofSeconds(Long.parseLong(value));
        } catch (NumberFormatException e) {
            log.warn("응답 기한을 읽지 못해 기본값을 쓴다: value={}", value);
            return fallback;
        }
    }
}

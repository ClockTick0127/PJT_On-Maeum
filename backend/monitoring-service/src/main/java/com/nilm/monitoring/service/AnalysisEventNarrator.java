package com.nilm.monitoring.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nilm.monitoring.config.enums.ApplianceType;
import com.nilm.monitoring.domain.AnalysisEvent;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 위험 이벤트를 화면 문구와 판단 근거로 풀어낸다.
 * 이상 징후 목록과 실시간 스트림이 같은 문구를 쓰도록 한곳에서만 관리한다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AnalysisEventNarrator {

    /**
     * 이벤트 유형별 화면 문구. 분석 서비스가 새 유형을 추가해도 화면이 비지 않도록
     * 표에 없는 값은 일반 문구로 되돌린다.
     */
    private static final Map<String, String> DESCRIPTIONS = Map.of(
            "ROUTINE_MISSED", "%s 미작동 감지",
            "PROLONGED_APPLIANCE_USE", "%s 장시간 사용 감지",
            "PROLONGED_INACTIVITY", "장시간 무활동 감지"
    );

    private final ObjectMapper objectMapper;

    public String describe(AnalysisEvent event) {
        String template = event.getEventType() == null
                ? null
                : DESCRIPTIONS.get(event.getEventType());
        if (template == null) {
            return event.getApplianceType() == null || event.getApplianceType().isBlank()
                    ? "이상 징후 감지"
                    : ApplianceType.labelOf(event.getApplianceType()) + " 사용 이상 감지";
        }
        return String.format(template, ApplianceType.labelOf(event.getApplianceType()));
    }

    /**
     * 알림 본문. 유형별 문구에 reason의 수치를 붙여 담당자가 무엇이 근거였는지 알게 한다.
     *
     * <p>유형 무관 고정 문구를 쓰지 않는다는 것이 설계 11.4절의 요구다.
     * reason의 구조는 유형마다 다르고 분석 서비스가 바꿀 수 있으므로,
     * 값이 없거나 형식이 다르면 수치를 빼고 기본 문구만 돌려준다.
     */
    public String describeDetail(AnalysisEvent event) {
        String headline = describe(event);
        Map<String, Object> reason = parseReason(event);

        String detail = switch (event.getEventType() == null ? "" : event.getEventType()) {
            case "PROLONGED_APPLIANCE_USE" -> number(reason.get("allowed_duration_minutes"))
                    .map(minutes -> "허용 시간 %s분을 넘겼습니다.".formatted(trim(minutes)))
                    .orElse(null);
            case "PROLONGED_INACTIVITY" -> number(reason.get("threshold_hours"))
                    .map(hours -> "%s시간 넘게 활동이 없습니다.".formatted(trim(hours)))
                    .orElse(null);
            case "ROUTINE_MISSED" -> reason.get("expected_until") instanceof String until
                    ? "평소 %s까지는 사용하던 가전입니다.".formatted(until)
                    : null;
            default -> null;
        };

        return detail == null ? headline : headline + ". " + detail;
    }

    private Optional<Double> number(Object value) {
        if (value instanceof Number parsed) {
            return Optional.of(parsed.doubleValue());
        }
        if (value instanceof String text) {
            try {
                return Optional.of(Double.parseDouble(text));
            } catch (NumberFormatException e) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    /** 6.0시간이 아니라 6시간으로 읽히게 한다. */
    private String trim(double value) {
        return value == Math.rint(value)
                ? String.valueOf((long) value)
                : String.valueOf(value);
    }

    /**
     * reason은 분석 서비스가 보낸 JSON을 문자열로 보관한다.
     * 이벤트 유형마다 구조가 달라 화면에 그대로 넘기고, 깨진 값이면 원문을 담아 돌려준다.
     */
    public Map<String, Object> parseReason(AnalysisEvent event) {
        String reason = event.getReason();
        if (reason == null || reason.isBlank()) {
            return Map.of();
        }
        try {
            Map<String, Object> parsed = objectMapper.readValue(
                    reason, new TypeReference<Map<String, Object>>() {
                    });
            return parsed == null ? Map.of() : parsed;
        } catch (Exception e) {
            log.warn("이벤트 사유를 JSON으로 읽지 못했습니다: eventId={}", event.getId());
            return Map.of("raw", reason);
        }
    }
}

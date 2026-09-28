package com.nilm.monitoring.config;

import com.nilm.monitoring.config.enums.RiskLevel;
import com.nilm.monitoring.risk.RiskPolicy;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 위험 평가와 이벤트 라우팅이 쓰는 설정 한 벌.
 *
 * <p>설계 11.2·11.3절은 점수식과 임계값을 "미검증 정책안"이라고 못 박았다.
 * 그래서 어떤 값도 코드에 확정으로 박지 않고 전부 여기에서 바꿀 수 있게 두고,
 * 어떤 값으로 계산했는지는 {@code policyVersion}으로 평가 이력에 남긴다.
 *
 * <p>{@code eventRouting}은 {@code app.risk.event-routing.*}를 통째로 받는 표다.
 * 이벤트 유형은 미리 열거할 수 없으므로(분석 서비스가 새 유형을 추가한다)
 * 필드로 선언하지 않고 Map으로 받아 {@link EventRoutingPolicy}가 해석한다.
 */
@Getter
@Setter
@ConfigurationProperties("app.risk")
public class RiskProperties {

    /** 이 값들로 계산했다는 표시. 정책을 바꾸면 함께 올린다. */
    private String policyVersion = "policy-v2-experimental";

    /** 이벤트의 주의 등급을 저장 점수로 옮긴 대표값. */
    private int warningThreshold = 70;

    /** 이벤트의 위험 등급을 저장 점수로 옮긴 대표값. */
    private int dangerThreshold = 90;

    /** 이 z 이하는 지표 점수 0. 설계 11.2절의 a에 해당하는 실험값이다. */
    private double zLow = 2;

    /** 이 z 이상은 지표 점수 1. 설계 11.2절의 b에 해당하는 실험값이다. */
    private double zHigh = 6;

    /** 프로필 유효기간. 실제 판정은 수신 단계가 하고 여기에는 기록용으로 싣는다. */
    private Duration profileMaxAge = Duration.ofDays(3);

    /** 현재 관측이 이보다 오래되면 활동량 지표를 계산하지 않는다. */
    private Duration observationMaxAge = Duration.ofMinutes(15);

    /** 같은 등급이 이어질 때 알림을 다시 보내기까지의 최소 간격. */
    private Duration realertInterval = Duration.ofHours(1);

    /** 비교 통계 한 줄을 믿기 위한 최소 표본 수. */
    private int minSampleCount = 7;

    /** 신뢰도 1.0에 도달하는 유효 관측일 수. */
    private int minEligibleDays = 14;

    /**
     * 하루치 단정("오늘 한 번도 쓰지 않았다")을 세우기 위한 최소 관측 커버리지 비율.
     * Gold가 기준선 표본으로 받아들이는 관측일의 커버리지 하한(0.95)과 같은 값으로 둔다.
     * 비교 대상이 완전히 관측된 날들의 분포이기 때문이다.
     */
    private double minObservationCoverage = 0.95;

    /**
     * 스냅샷 사이가 이보다 벌어지면 그 구간은 보지 못한 구간이다.
     * 분석 서비스의 데이터 공백 기준(app.power-usage.gap-threshold)과 같은 값으로 둔다.
     */
    private Duration observationGapThreshold = Duration.ofSeconds(120);

    /** 같은 결과가 이어질 때 평가 이력을 남기는 간격. */
    private Duration assessmentLogInterval = Duration.ofMinutes(30);

    /** 이벤트 등급 슬롯의 최대 유지시간. 해제 신호를 놓쳤을 때의 안전장치다. */
    private Duration eventLevelMaxHold = Duration.ofHours(6);

    private boolean schedulerEnabled = true;

    private Map<String, String> eventRouting = new LinkedHashMap<>();

    /** 계산기에 넘길 정책 값으로 옮긴다. */
    public RiskPolicy toPolicy() {
        return new RiskPolicy(
                policyVersion,
                warningThreshold,
                dangerThreshold,
                zLow,
                zHigh,
                profileMaxAge,
                observationMaxAge,
                realertInterval,
                minSampleCount,
                minEligibleDays,
                minObservationCoverage
        );
    }

    /** 등급 하나를 저장 점수로 옮긴 대표값. 이벤트 계약에는 score가 없다. */
    public int representativeScore(RiskLevel level) {
        return switch (level) {
            case DANGER -> dangerThreshold;
            case WARNING -> warningThreshold;
            case NORMAL -> 0;
        };
    }
}

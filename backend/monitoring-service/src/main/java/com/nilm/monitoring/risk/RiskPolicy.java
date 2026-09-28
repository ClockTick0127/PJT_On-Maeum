package com.nilm.monitoring.risk;

import java.time.Duration;

/**
 * 위험 평가 한 벌이 쓰는 정책 값.
 *
 * <p>설계 11.2·11.3절이 "미검증 정책안"이라고 못 박은 값들이다.
 * 그래서 기본값을 코드에 박지 않고 {@code application.properties}에서 전부 조정할 수 있게 두고,
 * 어떤 값으로 계산했는지 {@code policyVersion}으로 평가 이력에 남긴다.
 *
 * @param warningThreshold 주의 등급을 저장 점수로 옮길 때의 대표값. 자체 평가는 등급을 내지 않는다
 * @param dangerThreshold 위험 등급을 저장 점수로 옮길 때의 대표값
 * @param zLow 이 z 이하는 지표 점수 0
 * @param zHigh 이 z 이상은 지표 점수 1
 * @param profileMaxAge 프로필 유효기간. 판정 자체는 수신 단계가 하고 여기에는 기록용으로 싣는다
 * @param observationMaxAge 현재 관측이 이보다 오래되면 활동량 지표를 계산하지 않는다
 * @param realertInterval 같은 등급이 이어질 때 알림을 다시 보내기까지의 최소 간격
 * @param minSampleCount 비교 통계 한 줄을 믿기 위한 최소 표본 수
 * @param minEligibleDays 신뢰도 1.0에 도달하는 유효 관측일 수
 * @param minObservationCoverage 하루치 단정을 세우기 위한 최소 관측 커버리지 비율
 */
public record RiskPolicy(
        String policyVersion,
        int warningThreshold,
        int dangerThreshold,
        double zLow,
        double zHigh,
        Duration profileMaxAge,
        Duration observationMaxAge,
        Duration realertInterval,
        int minSampleCount,
        int minEligibleDays,
        double minObservationCoverage
) {

    public RiskPolicy {
        if (warningThreshold < 0 || dangerThreshold > 100 || warningThreshold >= dangerThreshold) {
            throw new IllegalStateException("위험 임계치는 0 <= 주의 < 위험 <= 100이어야 합니다.");
        }
        if (zHigh <= zLow) {
            throw new IllegalStateException("z 상한은 하한보다 커야 합니다.");
        }
        if (minEligibleDays <= 0) {
            throw new IllegalStateException("신뢰도 기준 관측일 수는 1 이상이어야 합니다.");
        }
        if (minObservationCoverage <= 0 || minObservationCoverage > 1) {
            throw new IllegalStateException("최소 관측 커버리지는 0 초과 1 이하여야 합니다.");
        }
    }
}

package com.nilm.monitoring.risk;

import com.nilm.monitoring.config.enums.RiskLevel;
import java.util.List;

/**
 * 평가 한 번의 결과.
 *
 * <p>점수는 nullable이다. 설계 11.1절대로 "계산하지 못했다"를 0점·정상으로 바꾸지 않는다.
 *
 * <p>등급은 {@code monitoring-score-v2-A}부터 언제나 null이다. 자체 평가는 참고 점수만 내고
 * 등급은 분석 서비스 이벤트가 정한다. 필드를 남겨 둔 것은 평가 이력과 재평가 출력의
 * 칸을 v1과 같게 유지하기 위해서다.
 *
 * @param confidence 0~1. 점수에 곱하지 않는다. 데이터 부족을 낮은 위험으로 둔갑시키지 않기 위해서다
 */
public record RiskAssessment(
        Integer score,
        RiskLevel level,
        AssessmentStatus status,
        double confidence,
        List<IndicatorResult> indicators,
        String profileVersion,
        String policyVersion,
        String scoreVersion
) {
}

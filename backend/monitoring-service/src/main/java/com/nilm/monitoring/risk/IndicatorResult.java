package com.nilm.monitoring.risk;

/**
 * 지표 하나의 계산 결과.
 *
 * <p>점수만 남기면 나중에 왜 그 점수였는지 되짚을 수 없다.
 * 설계 12장이 요구한 대로 관측값과 비교 통계값, 제외 사유까지 함께 보관한다.
 *
 * @param code A(활동 감소). v1 이력에는 M(루틴 미사용)·I(무활동)도 남아 있다
 * @param score 0~1. 제외됐으면 null
 * @param observed 이 지표가 본 현재 값
 * @param compareCenter 비교 기준(중앙값)
 * @param compareSpread 비교에 쓴 변동폭
 * @param bucket 비교에 쓴 시간대 구간
 * @param excludedReason 제외 사유. 계산했으면 null
 */
public record IndicatorResult(
        String code,
        Double score,
        Double observed,
        Double compareCenter,
        Double compareSpread,
        String bucket,
        String excludedReason
) {

    public boolean included() {
        return excludedReason == null && score != null;
    }

    static IndicatorResult excluded(String code, String reason) {
        return new IndicatorResult(code, null, null, null, null, null, reason);
    }
}

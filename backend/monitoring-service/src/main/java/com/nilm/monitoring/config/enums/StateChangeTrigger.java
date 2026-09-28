package com.nilm.monitoring.config.enums;

/**
 * 대상자 상태가 바뀐 원인.
 * 담당자 대시보드가 갱신 이유를 구분할 수 있게 실시간 이벤트에 함께 담는다.
 */
public enum StateChangeTrigger {

    /** 분석 결과(위험 이벤트) 반영 */
    DETECTION,

    /** 대상자가 안전 확인 알림에 응답 */
    SUBJECT_RESPONSE,

    /** 대상자가 응답하지 않아 기한이 만료됨 */
    RESPONSE_EXPIRED,

    /** 담당자가 알림 처리 상태를 변경 */
    MANAGER_STATUS,

    /** 외출 모드가 시작되거나 끝남 */
    AWAY_MODE,

    /** 가전 ON→OFF 전환으로 마지막 활동이 갱신됨 */
    ACTIVITY,

    /** Gold 배치가 만든 생활 프로필의 최신 버전이 반영됨 */
    PROFILE_UPDATED,

    /** 모니터링이 스스로 계산한 위험 평가가 반영됨 */
    ASSESSMENT
}

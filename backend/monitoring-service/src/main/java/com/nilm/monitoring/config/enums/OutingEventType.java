package com.nilm.monitoring.config.enums;

/**
 * AI 분석 서비스로 내보내는 가구 외출 이벤트 종류.
 *
 * 이름이 그대로 {@code monitoring.household-presence.v1}의
 * {@code event_type} 값이 되므로 계약을 바꾸지 않는 한 변경하지 않는다.
 */
public enum OutingEventType {

    /** 외출이 시작됨(감시 보류) */
    OUTING_STARTED,

    /** 귀가로 외출이 끝남(감시 재개) */
    OUTING_ENDED
}

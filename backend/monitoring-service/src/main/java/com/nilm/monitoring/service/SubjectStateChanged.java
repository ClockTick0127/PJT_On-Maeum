package com.nilm.monitoring.service;

import com.nilm.monitoring.config.enums.StateChangeTrigger;

/**
 * 대상자 상태가 바뀌었다는 내부 신호.
 * 커밋된 뒤에 실시간 스트림으로 흘려보내려고 쓰며, 값은 듣는 쪽에서 DB로 다시 읽는다.
 */
public record SubjectStateChanged(Long subjectId, StateChangeTrigger trigger) {
}

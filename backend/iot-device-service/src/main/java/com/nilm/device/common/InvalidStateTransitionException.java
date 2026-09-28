package com.nilm.device.common;

/** 허용되지 않은 기기 상태 전이 — DomainExceptionHandler가 400으로 변환. */
public class InvalidStateTransitionException extends RuntimeException {

    public InvalidStateTransitionException(String from, String to) {
        super("허용되지 않은 상태 전이입니다: %s → %s".formatted(from, to));
    }
}

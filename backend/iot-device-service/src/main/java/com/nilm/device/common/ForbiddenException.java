package com.nilm.device.common;

/** 인증은 됐으나 해당 자원에 대한 권한이 없음 — DomainExceptionHandler가 403으로 변환. */
public class ForbiddenException extends RuntimeException {

    public ForbiddenException(String message) {
        super(message);
    }
}

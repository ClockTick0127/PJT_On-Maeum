package com.nilm.device.common;

/** 리소스 상태상 허용되지 않는 조작 — DomainExceptionHandler가 400으로 변환. */
public class InvalidOperationException extends RuntimeException {

    public InvalidOperationException(String message) {
        super(message);
    }
}

package com.nilm.device.common;

/** 현재 사용자를 식별할 수 없음 — DomainExceptionHandler가 401로 변환. */
public class UnauthenticatedException extends RuntimeException {

    public UnauthenticatedException(String message) {
        super(message);
    }
}

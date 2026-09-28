package com.nilm.device.common;

/** 이미 존재하는 리소스 등록 시도 — DomainExceptionHandler가 409로 변환. */
public class DuplicateResourceException extends RuntimeException {

    public DuplicateResourceException(String resource, Object id) {
        super("%s(이)가 이미 존재합니다: %s".formatted(resource, id));
    }
}

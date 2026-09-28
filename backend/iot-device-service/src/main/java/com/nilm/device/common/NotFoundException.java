package com.nilm.device.common;

/** 도메인 리소스를 찾지 못한 경우 — DomainExceptionHandler가 404로 변환. */
public class NotFoundException extends RuntimeException {

    public NotFoundException(String resource, Object id) {
        super("%s(을)를 찾을 수 없습니다: %s".formatted(resource, id));
    }
}

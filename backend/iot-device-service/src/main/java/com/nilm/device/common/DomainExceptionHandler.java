package com.nilm.device.common;

import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 도메인 예외 전용 핸들러 — 공통 GlobalExceptionHandler는 수정하지 않고
 * 도메인 계층의 예외만 여기서 HTTP 응답으로 변환한다.
 */
@Order(0)
@RestControllerAdvice
public class DomainExceptionHandler {

    @ExceptionHandler(NotFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(NotFoundException e, HttpServletRequest request) {
        return build(HttpStatus.NOT_FOUND, "RESOURCE_NOT_FOUND", e.getMessage(), request);
    }

    @ExceptionHandler(InvalidStateTransitionException.class)
    public ResponseEntity<ErrorResponse> handleInvalidTransition(
            InvalidStateTransitionException e, HttpServletRequest request) {
        return build(HttpStatus.BAD_REQUEST, "INVALID_STATE_TRANSITION", e.getMessage(), request);
    }

    @ExceptionHandler(InvalidOperationException.class)
    public ResponseEntity<ErrorResponse> handleInvalidOperation(
            InvalidOperationException e, HttpServletRequest request) {
        return build(HttpStatus.BAD_REQUEST, "INVALID_OPERATION", e.getMessage(), request);
    }

    @ExceptionHandler(DuplicateResourceException.class)
    public ResponseEntity<ErrorResponse> handleDuplicate(
            DuplicateResourceException e, HttpServletRequest request) {
        return build(HttpStatus.CONFLICT, "DUPLICATE_RESOURCE", e.getMessage(), request);
    }

    @ExceptionHandler(UnauthenticatedException.class)
    public ResponseEntity<ErrorResponse> handleUnauthenticated(
            UnauthenticatedException e, HttpServletRequest request) {
        return build(HttpStatus.UNAUTHORIZED, "UNAUTHENTICATED", e.getMessage(), request);
    }

    @ExceptionHandler(ForbiddenException.class)
    public ResponseEntity<ErrorResponse> handleForbidden(
            ForbiddenException e, HttpServletRequest request) {
        return build(HttpStatus.FORBIDDEN, "FORBIDDEN", e.getMessage(), request);
    }

    private ResponseEntity<ErrorResponse> build(HttpStatus status, String code,
                                                String message, HttpServletRequest request) {
        return ResponseEntity.status(status).body(ErrorResponse.of(
                status.value(), code, message, request.getRequestURI(), List.of()));
    }
}

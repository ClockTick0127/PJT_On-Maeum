package com.nilm.monitoring.common;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import org.apache.catalina.connector.ClientAbortException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.async.AsyncRequestNotUsableException;
import org.springframework.web.context.request.async.AsyncRequestTimeoutException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.resource.NoResourceFoundException;
import org.springframework.web.server.ResponseStatusException;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleMethodArgumentNotValid(
            MethodArgumentNotValidException e, HttpServletRequest request) {
        List<ErrorResponse.FieldErrorDetail> errors = e.getBindingResult().getFieldErrors().stream()
                .map(fe -> new ErrorResponse.FieldErrorDetail(fe.getField(), fe.getDefaultMessage()))
                .toList();
        return ResponseEntity.badRequest().body(ErrorResponse.of(
                HttpStatus.BAD_REQUEST.value(), "VALIDATION_ERROR",
                "입력값 검증에 실패했습니다.", request.getRequestURI(), errors));
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ErrorResponse> handleConstraintViolation(
            ConstraintViolationException e, HttpServletRequest request) {
        List<ErrorResponse.FieldErrorDetail> errors = e.getConstraintViolations().stream()
                .map(v -> new ErrorResponse.FieldErrorDetail(v.getPropertyPath().toString(), v.getMessage()))
                .toList();
        return ResponseEntity.badRequest().body(ErrorResponse.of(
                HttpStatus.BAD_REQUEST.value(), "VALIDATION_ERROR",
                "입력값 검증에 실패했습니다.", request.getRequestURI(), errors));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleHttpMessageNotReadable(
            HttpMessageNotReadableException e,
            HttpServletRequest request
    ) {
        return ResponseEntity.badRequest().body(ErrorResponse.of(
                HttpStatus.BAD_REQUEST.value(),
                "MALFORMED_REQUEST",
                "요청 본문을 읽을 수 없습니다.",
                request.getRequestURI(),
                List.of()
        ));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ErrorResponse> handleTypeMismatch(
            MethodArgumentTypeMismatchException e,
            HttpServletRequest request
    ) {
        return ResponseEntity.badRequest().body(ErrorResponse.of(
                HttpStatus.BAD_REQUEST.value(),
                "VALIDATION_ERROR",
                "입력값 검증에 실패했습니다.",
                request.getRequestURI(),
                List.of(new ErrorResponse.FieldErrorDetail(
                        e.getName(), "형식이 올바르지 않습니다."))));
    }

    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<ErrorResponse> handleNoResourceFound(
            NoResourceFoundException e, HttpServletRequest request) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ErrorResponse.of(
                HttpStatus.NOT_FOUND.value(), "NOT_FOUND",
                "요청한 리소스를 찾을 수 없습니다.", request.getRequestURI(), List.of()));
    }

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ErrorResponse> handleResponseStatus(
            ResponseStatusException e,
            HttpServletRequest request
    ) {
        return ResponseEntity.status(e.getStatusCode())
                .body(ErrorResponse.of(
                        e.getStatusCode().value(),
                        "REQUEST_REJECTED",
                        e.getReason() == null
                                ? "요청을 처리할 수 없습니다."
                                : e.getReason(),
                        request.getRequestURI(),
                        List.of()
                ));
    }

    /**
     * 클라이언트가 먼저 끊은 스트림(SSE 탭 닫힘, 프록시 타임아웃, 응답 도중 연결 종료).
     * 받을 상대가 없으므로 본문을 쓰지 않고(void) 한 줄만 남긴다.
     * 본문을 쓰려 하면 Content-Type이 text/event-stream으로 확정된 응답에
     * JSON 변환기가 없다는 WARN이 한 번 더 찍힌다.
     */
    @ExceptionHandler({
            AsyncRequestNotUsableException.class,
            AsyncRequestTimeoutException.class,
            ClientAbortException.class
    })
    public void handleClientGone(Exception e, HttpServletRequest request) {
        log.debug("클라이언트가 먼저 끊은 요청: {} ({}: {})",
                request.getRequestURI(), e.getClass().getSimpleName(), e.getMessage());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpected(Exception e, HttpServletRequest request) {
        log.error("Unexpected error at {}", request.getRequestURI(), e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.of(
                HttpStatus.INTERNAL_SERVER_ERROR.value(), "INTERNAL_SERVER_ERROR",
                "서버 내부 오류가 발생했습니다.", request.getRequestURI(), List.of()));
    }
}

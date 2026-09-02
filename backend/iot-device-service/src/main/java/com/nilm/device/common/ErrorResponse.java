package com.nilm.device.common;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * 모든 서비스가 동일한 형태로 반환하는 공통 오류 응답.
 */
public record ErrorResponse(
        OffsetDateTime timestamp,
        int status,
        String code,
        String message,
        String path,
        List<FieldErrorDetail> errors
) {

    public record FieldErrorDetail(String field, String reason) {
    }

    public static ErrorResponse of(int status, String code, String message, String path,
                                   List<FieldErrorDetail> errors) {
        return new ErrorResponse(OffsetDateTime.now(), status, code, message, path,
                errors == null ? List.of() : errors);
    }
}

package com.nilm.monitoring.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.time.LocalDate;

public record SubjectCreateRequest(

        @NotBlank(message = "이름은 필수입니다.")
        @Size(max = 50, message = "이름은 50자 이하여야 합니다.")
        String name,

        @NotNull(message = "생년월일은 필수입니다.")
        LocalDate birthDate,

        @NotBlank(message = "전화번호는 필수입니다.")
        @Pattern(
                regexp = "^0[0-9]{8,10}$",
                message = "전화번호는 0으로 시작하는 숫자 9~11자리여야 합니다."
        )
        String phone,

        @NotBlank(message = "주소는 필수입니다.")
        @Size(max = 255, message = "주소는 255자 이하여야 합니다.")
        String address,

        @Size(max = 100, message = "상세 주소는 100자 이하여야 합니다.")
        String addressDetail,

        @NotBlank(message = "가구 ID는 필수입니다.")
        @Size(max = 10, message = "가구 ID는 10자 이하여야 합니다.")
        String householdId,

        @Size(max = 1000, message = "메모는 1000자 이하여야 합니다.")
        String managerMemo
) {

    // 생성자에서 정규화한 값에 대해 @Valid 검증을 수행한다.
    public SubjectCreateRequest {
        name = strip(name);
        phone = normalizePhone(phone);
        address = strip(address);
        addressDetail = nullableText(addressDetail);
        householdId = strip(householdId);
        managerMemo = nullableText(managerMemo);
    }

    private static String strip(String value) {
        return value == null ? null : value.strip();
    }

    private static String nullableText(String value) {
        String normalized = strip(value);

        return normalized == null || normalized.isBlank()
                ? null
                : normalized;
    }

    private static String normalizePhone(String value) {
        if (value == null) {
            return null;
        }

        return value.strip()
                .replace("-", "")
                .replaceAll("\\s", "");
    }
}
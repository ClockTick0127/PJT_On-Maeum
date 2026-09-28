package com.nilm.monitoring.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Size;

/** 전달된 값만 변경하는 담당자 계정 및 알림 설정 요청. */
public record ManagerProfileUpdateRequest(
        Boolean pushEnabled,
        Boolean dailyReportEnabled,

        @Email(message = "이메일 형식이 올바르지 않습니다.")
        @Size(min = 1, message = "이메일은 공백일 수 없습니다.")
        @Size(max = 100, message = "이메일은 100자 이하여야 합니다.")
        String email,

        @Size(min = 1, message = "기관명은 공백일 수 없습니다.")
        @Size(max = 100, message = "기관명은 100자 이하여야 합니다.")
        String organization
) {
    public ManagerProfileUpdateRequest {
        email = email == null ? null : email.strip();
        organization = organization == null ? null : organization.strip();
    }

    public boolean hasChanges() {
        return pushEnabled != null
                || dailyReportEnabled != null
                || email != null
                || organization != null;
    }
}

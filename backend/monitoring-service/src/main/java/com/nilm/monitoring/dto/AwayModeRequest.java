package com.nilm.monitoring.dto;

import jakarta.validation.constraints.NotNull;

import java.time.OffsetDateTime;

/**
 * 외출 모드 설정 요청.
 *
 * <p>{@code startsAt}을 생략하면 즉시 시작하고, {@code endsAt}을 생략하면
 * 해제할 때까지 이어진다. 두 값 모두 오프셋이 포함된 시각으로 받는다.
 */
public record AwayModeRequest(

        @NotNull(message = "외출 모드 사용 여부는 필수입니다.")
        Boolean enabled,

        OffsetDateTime startsAt,

        OffsetDateTime endsAt
) {

    /** 해제 요청에 시간이 함께 오면 클라이언트가 값을 잘못 보낸 것이다. */
    public boolean hasSchedule() {
        return startsAt != null || endsAt != null;
    }
}

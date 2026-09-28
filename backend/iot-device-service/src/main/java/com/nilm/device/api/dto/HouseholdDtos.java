package com.nilm.device.api.dto;

import com.nilm.device.domain.Household;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.time.OffsetDateTime;

public final class HouseholdDtos {

    private HouseholdDtos() {
    }

    public record CreateRequest(
            @NotBlank @Pattern(regexp = "^H\\d{3}$", message = "house_id는 H001 형식이어야 합니다")
            String houseId,
            @NotBlank @Size(max = 50)
            String alias,
            @Min(0) @Max(1440)
            Integer graceMinutes
    ) {
    }

    public record Response(
            String houseId,
            String alias,
            int graceMinutes,
            OffsetDateTime awayUntil,
            OffsetDateTime createdAt
    ) {
        public static Response from(Household h) {
            return new Response(h.getHouseId(), h.getAlias(), h.getGraceMinutes(),
                    h.getAwayUntil(), h.getCreatedAt());
        }
    }
}

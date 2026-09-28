package com.nilm.device.api.dto;

import com.nilm.device.domain.HouseholdInvite;
import com.nilm.device.domain.HouseholdMember;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.time.OffsetDateTime;
import java.util.UUID;

public final class InviteDtos {

    private InviteDtos() {
    }

    public record CreateRequest(
            @NotNull
            HouseholdMember.Relation relation,
            @Min(1) @Max(336)
            Integer expiresInHours
    ) {
    }

    public record AcceptRequest(
            @Size(max = 20)
            String notifyPhone
    ) {
    }

    public record Response(
            String code,
            String houseId,
            HouseholdMember.Relation relation,
            OffsetDateTime expiresAt,
            OffsetDateTime usedAt,
            UUID usedBy,
            OffsetDateTime revokedAt
    ) {
        public static Response from(HouseholdInvite i) {
            return new Response(i.getCode(), i.getHouseId(), i.getRelation(), i.getExpiresAt(),
                    i.getUsedAt(), i.getUsedBy(), i.getRevokedAt());
        }
    }
}

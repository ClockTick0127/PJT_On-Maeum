package com.nilm.device.api.dto;

import com.nilm.device.domain.HouseholdMember;
import jakarta.validation.constraints.Size;
import java.time.OffsetDateTime;
import java.util.UUID;

public final class MemberDtos {

    private MemberDtos() {
    }

    public record Response(
            String houseId,
            UUID keycloakUserId,
            HouseholdMember.Relation relation,
            HouseholdMember.NotifyPriority notifyPriority,
            String notifyPhone,
            boolean notifyEnabled,
            OffsetDateTime joinedAt
    ) {
        public static Response from(HouseholdMember m) {
            return new Response(m.getHouseId(), m.getKeycloakUserId(), m.getRelation(),
                    m.getNotifyPriority(), m.getNotifyPhone(), m.isNotifyEnabled(), m.getJoinedAt());
        }
    }

    /** 내 가구 목록 한 줄 — 가구 별칭과 내 관계 */
    public record MyHousehold(
            String houseId,
            String alias,
            HouseholdMember.Relation relation,
            HouseholdMember.NotifyPriority notifyPriority,
            boolean notifyEnabled
    ) {
    }

    public record UpdateRequest(
            HouseholdMember.Relation relation,
            HouseholdMember.NotifyPriority notifyPriority,
            @Size(max = 20)
            String notifyPhone,
            Boolean notifyEnabled
    ) {
    }
}

package com.nilm.monitoring.dto;

import com.nilm.monitoring.domain.Subject;

import java.time.OffsetDateTime;

public record MyDashboardResponse(
        String subjectId,
        String name,
        AwayMode awayMode,
        ManagerSummary manager
) {

    /**
     * @param enabled   지금 외출 중인가
     * @param scheduled 아직 시작하지 않은 예약이 걸려 있는가
     * @param startedAt 외출 시작 시각(진행 중이거나 예약이 있을 때만)
     * @param until     외출 종료 시각(정해지지 않았으면 null)
     */
    public record AwayMode(
            boolean enabled,
            boolean scheduled,
            OffsetDateTime startedAt,
            OffsetDateTime until
    ) {

        public static AwayMode from(Subject subject, OffsetDateTime now) {
            boolean away = subject.isAwayAt(now);
            boolean scheduled = subject.hasScheduledAway(now);
            boolean hasWindow = away || scheduled;

            return new AwayMode(
                    away,
                    scheduled,
                    hasWindow ? subject.getAwayStartedAt() : null,
                    hasWindow ? subject.getAwayUntil() : null
            );
        }
    }

    public record ManagerSummary(
            String name,
            String phone
    ) {
    }
}

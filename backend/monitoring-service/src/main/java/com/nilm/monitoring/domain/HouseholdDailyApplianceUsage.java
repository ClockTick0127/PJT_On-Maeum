package com.nilm.monitoring.domain;

import jakarta.persistence.*;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import lombok.Getter;

/**
 * 가구·가전·영업일(KST)별 당일 사용 사실.
 *
 * <p>{@link ApplianceUsageEpisode}에서 다시 계산한 투영이다. 스냅샷 전환을 세던 때와 달리
 * Gold의 유효 사용 정의(병합 간격, 최소 사용시간)를 통과한 사용만 들어온다.
 *
 * <ul>
 *   <li>{@code firstOnAt}은 그 날의 첫 유효 사용 시각이다. 자정을 넘겨 이어진 사용은
 *       그 날의 시작 시각으로 잘라 적는다. 배치의 {@code first_use_at}과 같은 규칙이다.</li>
 *   <li>{@code startCount}는 그 날 <b>시작한</b> 유효 사용의 수다. 자정을 넘겨 이어진 사용은
 *       시작한 날에만 들어 있고 새 날의 시작으로 다시 세지 않는다.</li>
 * </ul>
 *
 * <p>그래서 "오늘 썼는가"와 "오늘 몇 번 시작했는가"는 다른 값일 수 있다. 자정을 넘긴
 * 사용만 있는 날은 사용 사실은 참이고 시작 횟수는 0이다.
 *
 * <p>평가의 현재 입력은 에피소드에서 직접 읽는다. 이 표는 같은 사실을 날짜로 접어 둔
 * 조회용 투영이라, 증분으로 더하지 않고 언제나 에피소드에서 다시 계산해 덮어쓴다.
 */
@Entity
@Getter
@Table(name = "household_daily_appliance_usage")
@IdClass(HouseholdDailyApplianceUsageId.class)
public class HouseholdDailyApplianceUsage {

    @Id
    @Column(name = "household_id", nullable = false)
    private String householdId;

    @Id
    @Column(name = "appliance_type", nullable = false, length = 50)
    private String applianceType;

    @Id
    @Column(name = "usage_date", nullable = false)
    private LocalDate usageDate;

    @Column(name = "first_on_at", nullable = false)
    private OffsetDateTime firstOnAt;

    @Column(name = "last_on_at", nullable = false)
    private OffsetDateTime lastOnAt;

    @Column(name = "start_count", nullable = false)
    private int startCount;

    protected HouseholdDailyApplianceUsage() {
    }

    public HouseholdDailyApplianceUsage(
            String householdId,
            String applianceType,
            LocalDate usageDate,
            OffsetDateTime firstOnAt,
            OffsetDateTime lastOnAt,
            int startCount
    ) {
        this.householdId = householdId;
        this.applianceType = applianceType;
        this.usageDate = usageDate;
        this.firstOnAt = firstOnAt;
        this.lastOnAt = lastOnAt;
        this.startCount = startCount;
    }

    /** 에피소드에서 다시 계산한 값으로 덮어쓴다. */
    public void apply(OffsetDateTime firstOnAt, OffsetDateTime lastOnAt, int startCount) {
        this.firstOnAt = firstOnAt;
        this.lastOnAt = lastOnAt;
        this.startCount = startCount;
    }
}

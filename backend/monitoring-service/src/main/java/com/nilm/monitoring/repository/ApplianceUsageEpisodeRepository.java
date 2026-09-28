package com.nilm.monitoring.repository;

import com.nilm.monitoring.domain.ApplianceUsageEpisode;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ApplianceUsageEpisodeRepository
        extends JpaRepository<ApplianceUsageEpisode, Long> {

    /** 진행 중인 사용. 쓰기 경로가 "이미 켜져 있는가"를 판정하는 자리다. */
    List<ApplianceUsageEpisode> findByHouseholdIdAndApplianceTypeAndEndedAtIsNull(
            String householdId, String applianceType);

    /** 진행 중인 사용 전부. 스냅샷 한 건으로 켜져 있는 사용들의 유효성을 확인할 때 쓴다. */
    List<ApplianceUsageEpisode> findByHouseholdIdAndEndedAtIsNull(String householdId);

    /** 마지막으로 끝난 사용. 병합 간격 안이면 여기에 이어 붙인다. */
    List<ApplianceUsageEpisode> findByHouseholdIdAndApplianceTypeAndEndedAtIsNotNullOrderByEndedAtDesc(
            String householdId, String applianceType, Limit limit);

    /** 같은 시작 시각의 사용. 재전송된 스냅샷이 사용을 두 번 만들지 않게 한다. */
    Optional<ApplianceUsageEpisode> findByHouseholdIdAndApplianceTypeAndStartedAt(
            String householdId, String applianceType, OffsetDateTime startedAt);

    /**
     * 한 가전이 주어진 구간에 걸친 유효 사용들. 일별 사용 사실을 다시 계산할 때 쓴다.
     * 진행 중인 사용도 구간에 걸쳐 있으면 포함한다.
     */
    @Query("""
            select e from ApplianceUsageEpisode e
            where e.householdId = :householdId
              and e.applianceType = :applianceType
              and e.valid = true
              and e.startedAt < :until
              and (e.endedAt is null or e.endedAt > :from)
            order by e.startedAt asc
            """)
    List<ApplianceUsageEpisode> findValidOverlapping(
            @Param("householdId") String householdId,
            @Param("applianceType") String applianceType,
            @Param("from") OffsetDateTime from,
            @Param("until") OffsetDateTime until);

    /**
     * 평가가 볼 유효 사용들. 아직 진행 중이거나 기준 구간 뒤에 끝난 사용을 모두 준다.
     * 가전을 가리지 않는다. 활동 감소(A)는 가구 전체의 시작 횟수를 세기 때문이다.
     */
    @Query("""
            select e from ApplianceUsageEpisode e
            where e.householdId = :householdId
              and e.valid = true
              and (e.endedAt is null or e.endedAt >= :from)
            order by e.startedAt asc
            """)
    List<ApplianceUsageEpisode> findValidSince(
            @Param("householdId") String householdId,
            @Param("from") OffsetDateTime from);

    /**
     * 기준 시각 이전에 끝난 마지막 유효 사용. 무활동 경과의 기준점이라
     * 조회 구간 밖이어도 반드시 한 건은 찾아야 한다.
     */
    @Query("""
            select e from ApplianceUsageEpisode e
            where e.householdId = :householdId
              and e.valid = true
              and e.endedAt is not null
              and e.endedAt <= :at
            order by e.endedAt desc
            """)
    List<ApplianceUsageEpisode> findLatestValidEndedAtOrBefore(
            @Param("householdId") String householdId,
            @Param("at") OffsetDateTime at,
            Limit limit);

    /** 한 영업일에 시작한 유효 사용의 수. 자정을 넘겨 이어진 사용은 시작한 날에만 든다. */
    long countByHouseholdIdAndApplianceTypeAndBusinessDateAndValidTrueAndStartImputedFalse(
            String householdId, String applianceType, LocalDate businessDate);
}

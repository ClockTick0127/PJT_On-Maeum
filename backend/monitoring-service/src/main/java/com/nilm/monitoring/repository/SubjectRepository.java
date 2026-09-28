package com.nilm.monitoring.repository;
import com.nilm.monitoring.domain.Subject;
import jakarta.persistence.LockModeType;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
public interface SubjectRepository extends JpaRepository<Subject, Long> {

    boolean existsByHouseholdId(String householdId);

    List<Subject> findAllByManagerIdOrderByIdAsc(Long managerId);

    /**
     * 타이머 평가가 도는 순서. id 순으로 고정해 순회 결과를 재현 가능하게 둔다.
     *
     * <p>엔티티가 아니라 식별자만 읽는다. 타이머가 대상자를 먼저 영속성 컨텍스트에 올려 두면
     * 뒤이어 잠금을 잡아도 그 사이 커밋된 이벤트 경로의 변경이 보이지 않는다.
     * 잠금을 잡는 조회가 트랜잭션의 첫 읽기가 되도록 목록에는 id와 가구만 싣는다.
     */
    @Query("select s.id as id, s.householdId as householdId from Subject s order by s.id")
    List<SubjectTarget> findAllTargets();

    /**
     * 대상자가 속한 가구. 엔티티를 올리지 않고 문자열만 읽어
     * 잠금 이전에 오래된 Subject가 영속성 컨텍스트에 남지 않게 한다.
     */
    @Query("select s.householdId from Subject s where s.id = :subjectId")
    Optional<String> findHouseholdIdById(@Param("subjectId") Long subjectId);

    Optional<Subject> findByAuthSub(String authSub);

    @Modifying
    @Query("""
            update Subject s
            set s.stateVersion = s.stateVersion + 1,
                s.updatedAt = :updatedAt
            where s.id = :subjectId
            """)
    int touchState(
            @Param("subjectId") Long subjectId,
            @Param("updatedAt") OffsetDateTime updatedAt
    );

    /**
     * monitoring_enabled 캐시가 외출 구간과 어긋난 대상자만 찾는다.
     * 예약된 외출의 시작·종료를 발효시키는 스케줄러가 쓴다.
     */
    @Query("""
            select s from Subject s
            where (s.monitoringEnabled = true
                   and s.awayStartedAt is not null
                   and s.awayStartedAt <= :now
                   and (s.awayUntil is null or s.awayUntil > :now))
               or (s.monitoringEnabled = false
                   and (s.awayStartedAt is null
                        or s.awayStartedAt > :now
                        or (s.awayUntil is not null and s.awayUntil <= :now)))
            """)
    List<Subject> findAwayTransitionCandidates(@Param("now") OffsetDateTime now);

    /**
     * 가구의 대상자 행을 쓰기 잠금으로 읽는다.
     *
     * <p>대상자 상태를 바꾸는 모든 경로가 이 조회 하나만 쓴다. 분석 이벤트, 가전 스냅샷,
     * 프로필 반영, 타이머 평가가 같은 잠금 집합을 같은 순서(가구 안에서 id 오름차순)로
     * 잡으므로 한 가구 안에서는 언제나 한 트랜잭션만 판정한다. 잠금 범위가 가구이므로
     * 서로 다른 가구는 서로를 기다리지 않는다.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select s from Subject s where s.householdId = :householdId order by s.id")
    List<Subject> findHouseholdForUpdate(@Param("householdId") String householdId);

    /** 타이머 순회용 식별자 한 쌍. 엔티티를 올리지 않으려고 투영으로만 읽는다. */
    interface SubjectTarget {

        Long getId();

        String getHouseholdId();
    }
}


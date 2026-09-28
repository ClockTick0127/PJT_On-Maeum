package com.nilm.monitoring.repository;
import com.nilm.monitoring.domain.AnalysisEvent;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
public interface AnalysisEventRepository extends JpaRepository<AnalysisEvent, UUID> {
    List<AnalysisEvent> findAllBySubjectIdAndOccurredAtGreaterThanEqualAndOccurredAtLessThan(
            Long subjectId,
            OffsetDateTime from,
            OffsetDateTime until
    );

    /** 실시간 스트림이 내려보내는 최근 이상 징후 건수. */
    long countBySubjectIdAndOccurredAtGreaterThanEqual(
            Long subjectId,
            OffsetDateTime from
    );

    /** 가장 최근에 발생한 이상 징후 한 건. 같은 시각은 id로 순서를 고정한다. */
    @Query(value = """
            select e.*
            from analysis_events e
            where e.subject_id = :subjectId
            order by e.occurred_at desc, e.id desc
            limit 1
            """, nativeQuery = true)
    Optional<AnalysisEvent> findLatestBySubjectId(@Param("subjectId") Long subjectId);

    /**
     * 이상 징후 기록의 첫 페이지. 발생 시각 최신순이고, 같은 시각은 id로 순서를 고정해
     * 커서 페이지네이션이 같은 행을 건너뛰거나 중복해서 보여주지 않게 한다.
     */
    @Query(value = """
            select e.*
            from analysis_events e
            where e.subject_id = :subjectId
              and e.occurred_at >= :from
              and e.occurred_at < :until
            order by e.occurred_at desc, e.id desc
            limit :limit
            """, nativeQuery = true)
    List<AnalysisEvent> findFirstPage(
            @Param("subjectId") Long subjectId,
            @Param("from") OffsetDateTime from,
            @Param("until") OffsetDateTime until,
            @Param("limit") int limit
    );

    /** 커서가 가리키는 (발생 시각, id)보다 뒤에 오는 행만 읽는다. */
    @Query(value = """
            select e.*
            from analysis_events e
            where e.subject_id = :subjectId
              and e.occurred_at >= :from
              and e.occurred_at < :until
              and (e.occurred_at < :cursorOccurredAt
                   or (e.occurred_at = :cursorOccurredAt and e.id < :cursorId))
            order by e.occurred_at desc, e.id desc
            limit :limit
            """, nativeQuery = true)
    List<AnalysisEvent> findPageAfterCursor(
            @Param("subjectId") Long subjectId,
            @Param("from") OffsetDateTime from,
            @Param("until") OffsetDateTime until,
            @Param("cursorOccurredAt") OffsetDateTime cursorOccurredAt,
            @Param("cursorId") UUID cursorId,
            @Param("limit") int limit
    );
}

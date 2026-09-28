package com.nilm.monitoring.repository;

import com.nilm.monitoring.domain.RiskAssessmentRecord;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface RiskAssessmentRecordRepository
        extends JpaRepository<RiskAssessmentRecord, UUID> {

    /**
     * 마지막으로 저장한 평가 한 건.
     * 같은 결과가 이어질 때 이력을 얼마나 자주 남길지 판정하는 기준이 된다.
     */
    @Query(value = """
            select a.*
            from risk_assessments a
            where a.subject_id = :subjectId
            order by a.assessed_at desc, a.created_at desc
            limit 1
            """, nativeQuery = true)
    Optional<RiskAssessmentRecord> findLatestBySubjectId(@Param("subjectId") Long subjectId);

    /** 위험 추이 그래프가 읽는 기간 안의 자체 평가 점수. */
    List<RiskAssessmentRecord> findAllBySubjectIdAndAssessedAtGreaterThanEqualAndAssessedAtLessThan(
            Long subjectId,
            OffsetDateTime from,
            OffsetDateTime until
    );
}

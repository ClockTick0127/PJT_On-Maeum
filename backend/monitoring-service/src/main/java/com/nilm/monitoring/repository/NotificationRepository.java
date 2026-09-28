package com.nilm.monitoring.repository;

import com.nilm.monitoring.domain.Notification;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

// 동시 응답 -> 비관적락
public interface NotificationRepository extends JpaRepository<Notification, Long> {

    /**
     * 가장 최근 생성된 알림 한 건.
     *
     * <p>분석 이벤트를 join하지 않고 알림 행의 subject_id로 찾는다.
     * 모니터링 자체 평가가 만든 알림은 분석 이벤트에 걸리지 않아
     * join으로는 영영 잡히지 않기 때문이다.
     */
    @Query(value = """
            select n.*
            from notifications n
            where n.subject_id = :subjectId
            order by n.id desc
            limit 1
            """, nativeQuery = true)
    Optional<Notification> findLatestAlertBySubjectId(@Param("subjectId") Long subjectId);

    /**
     * 담당자가 아직 조치를 끝내지 않은 알림 수 = 미해결 사건 수.
     * 일일 요약처럼 대상자를 특정할 수 없는 알림은 제외된다.
     */
    @Query(value = """
            select count(*)
            from notifications n
            where n.subject_id = :subjectId
              and n.manager_response_status <> 'RESOLVED'
            """, nativeQuery = true)
    long countUnresolvedBySubjectId(@Param("subjectId") Long subjectId);

    // 이상 징후 목록에서 이벤트마다 연결된 알림을 한 번에 채운다.
    List<Notification> findAllByEventIdIn(Collection<UUID> eventIds);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select n from Notification n where n.id = :id")
    Optional<Notification> findForResponse(@Param("id") Long id);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    List<Notification> findAllByResponseStatusAndResponseDeadlineLessThanEqual(
            Notification.ResponseStatus status,
            OffsetDateTime now
    );
}

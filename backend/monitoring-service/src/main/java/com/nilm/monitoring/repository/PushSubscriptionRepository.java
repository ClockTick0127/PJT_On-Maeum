package com.nilm.monitoring.repository;

import com.nilm.monitoring.domain.PushSubscription;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface PushSubscriptionRepository extends JpaRepository<PushSubscription, Long> {

    // 구독 소유권 갱신
    // 다른 endpoint로 등록하면 새행 추가
    // 같은 endpoint 등록하면 auth_sub 갱신
    @Modifying
    @Query(value = """
            INSERT INTO push_subscriptions (
                auth_sub,
                endpoint,
                p256dh,
                auth
            )
            VALUES (
                :authSub,
                :endpoint,
                :p256dh,
                :auth
            )
            ON CONFLICT (endpoint)
            DO UPDATE SET
                auth_sub = EXCLUDED.auth_sub,
                p256dh = EXCLUDED.p256dh,
                auth = EXCLUDED.auth
            """, nativeQuery = true)
    int register(
            @Param("authSub") String authSub,
            @Param("endpoint") String endpoint,
            @Param("p256dh") String p256dh,
            @Param("auth") String auth
    );

    // 구독 삭제 메서드
    @Modifying
    @Query(value = """
        DELETE FROM push_subscriptions
        WHERE auth_sub = :authSub
          AND endpoint = :endpoint
        """, nativeQuery = true)
    int deleteOwnedSubscription(
            @Param("authSub") String authSub,
            @Param("endpoint") String endpoint
    );

    List<PushSubscription> findAllByAuthSub(String authSub);
}

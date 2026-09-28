package com.nilm.monitoring.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nilm.monitoring.config.PowerUsageProperties;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * 최신 스냅샷을 Redis에 둔다.
 *
 * <p>Redis 쓰기는 DB 트랜잭션 밖이라 함께 롤백되지 않는다. 그래서 기준점은 스냅샷을
 * 처리하기 전에 옮겨 둔다. 롤백되면 그 구간 하나의 전력량을 잃지만, 같은 스냅샷이
 * 재전송돼도 {@code observedAt}이 기준점보다 앞서 적산이 건너뛰어져 이중 계산은 없다.
 * 화면에 없던 전력량이 생기는 쪽보다 한 구간이 비는 쪽이 낫다.
 *
 * <p>Redis가 죽어도 스냅샷 소비 자체는 멈추지 않는다. 전력량 그래프만 그동안 비고,
 * 마지막 활동·위험 평가 같은 본래 경로는 그대로 돈다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(
        name = "app.power-usage.snapshot-store",
        havingValue = "redis",
        matchIfMissing = true
)
public class RedisLatestSnapshotStore implements LatestSnapshotStore {

    private static final String KEY_PREFIX = "monitoring:latest-snapshot:";

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final PowerUsageProperties properties;

    @Override
    public Optional<LatestSnapshot> load(String householdId) {
        try {
            String raw = redis.opsForValue().get(KEY_PREFIX + householdId);
            if (raw == null) {
                return Optional.empty();
            }
            return Optional.of(objectMapper.readValue(raw, LatestSnapshot.class));
        } catch (JsonProcessingException e) {
            // 형식이 깨진 값은 기준점으로 쓸 수 없다. 없는 것으로 보고 다음 스냅샷부터 다시 잡는다.
            log.warn("최신 스냅샷을 읽을 수 없다: householdId={}", householdId, e);
            return Optional.empty();
        } catch (RuntimeException e) {
            log.warn("Redis에서 최신 스냅샷을 가져오지 못했다: householdId={}", householdId, e);
            return Optional.empty();
        }
    }

    @Override
    public void save(String householdId, LatestSnapshot snapshot) {
        try {
            redis.opsForValue().set(
                    KEY_PREFIX + householdId,
                    objectMapper.writeValueAsString(snapshot),
                    properties.getSnapshotTtl()
            );
        } catch (JsonProcessingException | RuntimeException e) {
            log.warn("Redis에 최신 스냅샷을 쓰지 못했다: householdId={}", householdId, e);
        }
    }
}

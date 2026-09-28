package com.nilm.monitoring.service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Redis 없이 도는 환경용 보관소. 테스트가 쓴다.
 * 인스턴스마다 따로 들고 있으므로 여러 대로 띄우는 운영에서는 쓰지 않는다.
 */
@Component
@ConditionalOnProperty(name = "app.power-usage.snapshot-store", havingValue = "memory")
public class InMemoryLatestSnapshotStore implements LatestSnapshotStore {

    private final Map<String, LatestSnapshot> byHousehold = new ConcurrentHashMap<>();

    @Override
    public Optional<LatestSnapshot> load(String householdId) {
        return Optional.ofNullable(byHousehold.get(householdId));
    }

    @Override
    public void save(String householdId, LatestSnapshot snapshot) {
        byHousehold.put(householdId, snapshot);
    }

    /** 테스트가 가구 상태를 되돌릴 때 쓴다. */
    public void clear() {
        byHousehold.clear();
    }
}

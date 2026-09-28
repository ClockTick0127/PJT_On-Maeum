package com.nilm.monitoring.service;

import java.util.Optional;

/**
 * 가구별 최신 스냅샷 보관소.
 *
 * <p>DB가 아니라 별도 저장소에 두는 값이다. 스냅샷마다 갱신되는 데다 적분 기준점으로만 쓰여
 * 영구 보존할 이유가 없고, 가구 행 락을 잡지 않겠다는 스냅샷 경로의 전제와도 맞는다.
 */
public interface LatestSnapshotStore {

    Optional<LatestSnapshot> load(String householdId);

    void save(String householdId, LatestSnapshot snapshot);
}

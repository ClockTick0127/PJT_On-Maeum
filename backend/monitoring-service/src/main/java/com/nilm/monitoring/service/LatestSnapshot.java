package com.nilm.monitoring.service;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

/**
 * 적분의 기준점이 되는 직전 스냅샷.
 * 전력량은 두 시점 사이의 값이므로 앞선 관측을 들고 있어야 계산할 수 있다.
 */
public record LatestSnapshot(OffsetDateTime observedAt, BigDecimal activePower) {
}

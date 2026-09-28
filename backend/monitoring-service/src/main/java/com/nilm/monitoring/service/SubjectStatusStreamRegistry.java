package com.nilm.monitoring.service;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * 담당자별로 열려 있는 SSE 연결을 들고 있는 저장소.
 * 한 담당자가 여러 탭을 열 수 있으므로 연결은 담당자 1명당 여러 개다.
 *
 * <p>연결은 인스턴스 메모리에만 있다. 서비스를 여러 대로 늘리면 구독자가 붙은
 * 인스턴스에서만 갱신이 나가므로, 그때는 Redis Pub/Sub 같은 전달 경로가 필요하다.
 */
@Slf4j
@Component
public class SubjectStatusStreamRegistry {

    private final Map<Long, Set<SseEmitter>> byManagerId = new ConcurrentHashMap<>();

    /**
     * 새 연결을 등록한다. 브라우저가 탭을 닫거나 프록시가 끊으면
     * 콜백에서 스스로 빠져나가 죽은 연결이 쌓이지 않는다.
     */
    public SseEmitter register(Long managerId, long timeoutMillis) {
        SseEmitter emitter = new SseEmitter(timeoutMillis);
        byManagerId
                .computeIfAbsent(managerId, key -> new CopyOnWriteArraySet<>())
                .add(emitter);

        emitter.onCompletion(() -> remove(managerId, emitter));
        emitter.onTimeout(() -> {
            remove(managerId, emitter);
            emitter.complete();
        });
        emitter.onError(error -> remove(managerId, emitter));

        // 첫 이상 징후까지 몇 시간이 걸릴 수 있다. 주석 한 줄을 먼저 흘려 응답을 확정해
        // 브라우저의 onopen과 프록시의 연결 수립을 기다리게 하지 않는다.
        write(managerId, emitter, SseEmitter.event().comment("connected"));
        return emitter;
    }

    /** 담당자의 모든 연결로 이벤트를 보낸다. 끊긴 연결은 조용히 정리한다. */
    public void send(Long managerId, String eventName, Object payload) {
        for (SseEmitter emitter : emitters(managerId)) {
            write(managerId, emitter, SseEmitter.event().name(eventName).data(payload));
        }
    }

    /**
     * 유휴 연결을 프록시가 끊지 않도록 주석 한 줄을 흘려보낸다.
     * SSE 주석은 이벤트가 아니어서 브라우저의 onmessage를 깨우지 않는다.
     */
    public void heartbeat() {
        byManagerId.forEach((managerId, emitters) -> {
            for (SseEmitter emitter : emitters) {
                write(managerId, emitter, SseEmitter.event().comment("heartbeat"));
            }
        });
    }

    /** 구독자가 한 명도 없으면 갱신 payload를 만들 필요가 없다. */
    public boolean hasSubscriber(Long managerId) {
        return !emitters(managerId).isEmpty();
    }

    public int connectionCount(Long managerId) {
        return emitters(managerId).size();
    }

    private void write(Long managerId, SseEmitter emitter, SseEmitter.SseEventBuilder event) {
        try {
            emitter.send(event);
        } catch (IOException | IllegalStateException e) {
            // 이미 끊긴 연결이다. 남은 담당자들에게는 계속 보내야 하므로 삼킨다.
            // complete()는 부르지 않는다. 죽은 응답을 flush하려다 AsyncRequestNotUsableException이
            // 나고, 그 예외가 요청 스레드로 디스패치되어 500 처리기까지 타게 된다.
            // 전송에 실패한 emitter는 Spring이 이미 실패로 표시하고 onError 콜백을 부른다.
            log.debug("SSE 전송 실패로 연결을 정리합니다: managerId={}", managerId);
            remove(managerId, emitter);
        }
    }

    private Set<SseEmitter> emitters(Long managerId) {
        if (managerId == null) {
            return Set.of();
        }
        return byManagerId.getOrDefault(managerId, Set.of());
    }

    private void remove(Long managerId, SseEmitter emitter) {
        byManagerId.computeIfPresent(managerId, (key, emitters) -> {
            emitters.remove(emitter);
            return emitters.isEmpty() ? null : emitters;
        });
    }
}

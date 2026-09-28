package com.nilm.monitoring.service;

import com.nilm.monitoring.dto.PushSubscriptionRequest;
import com.nilm.monitoring.repository.PushSubscriptionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service @RequiredArgsConstructor
public class PushSubscriptionService {

    private final PushSubscriptionRepository repository;

    @Transactional
    public void register( // 소유권 충돌로 0행이 되는 분기가 없어지므로 void로 반환형 변경
            String authSub,
            PushSubscriptionRequest request
    ) {

        requireAuthSub(authSub);

        repository.register(
                authSub,
                request.endpoint(),
                request.keys().p256dh(),
                request.keys().auth()
        );

    }

    @Transactional
    public void unregister(String authSub, String endpoint) {
        requireAuthSub(authSub);
        repository.deleteOwnedSubscription(authSub, endpoint);
    }

    private void requireAuthSub(String authSub) {
        if (authSub == null || authSub.isBlank()) {
            throw new ResponseStatusException(
                    HttpStatus.UNAUTHORIZED,
                    "로그인이 필요합니다."
            );
        }
    }

}

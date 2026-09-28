package com.nilm.monitoring.api;

import com.nilm.monitoring.dto.PushSubscriptionDeleteRequest;
import com.nilm.monitoring.dto.PushSubscriptionRequest;
import com.nilm.monitoring.service.PushSubscriptionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/monitoring/push-subscriptions")
public class PushSubscriptionController {

    private final PushSubscriptionService service;

    @PostMapping
    public ResponseEntity<Void> register(
            @AuthenticationPrincipal Jwt jwt,
            @Valid @RequestBody PushSubscriptionRequest request
    ) {
        String authSub = jwt == null ? null : jwt.getSubject();
        service.register(authSub, request);

        return ResponseEntity.noContent().build();
    }

    @DeleteMapping
    public ResponseEntity<Void> unregister(
            @AuthenticationPrincipal Jwt jwt,
            @Valid @RequestBody PushSubscriptionDeleteRequest request
    ) {
        String authSub = jwt == null ? null : jwt.getSubject();
        service.unregister(authSub, request.endpoint());

        return ResponseEntity.noContent().build();
    }
}

package com.nilm.monitoring.api;

import com.nilm.monitoring.dto.ManagerProfileUpdateRequest;
import com.nilm.monitoring.service.ManagerProfileService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/monitoring")
public class ManagerProfileController {

    private final ManagerProfileService managerProfileService;

    @PatchMapping("/me")
    public ResponseEntity<Void> updateMe(
            @AuthenticationPrincipal Jwt jwt,
            @Valid @RequestBody ManagerProfileUpdateRequest request
    ) {
        String authSub = jwt == null ? null : jwt.getSubject();
        managerProfileService.update(authSub, request);
        return ResponseEntity.noContent().build();
    }
}

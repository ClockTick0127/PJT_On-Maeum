package com.nilm.device.security;

import com.nilm.device.common.UnauthenticatedException;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * 현재 요청의 사용자 식별자(Keycloak sub)를 꺼낸다.
 *
 * <p>보안이 꺼진 로컬 환경에서는 JWT가 없으므로 {@code X-User-Id} 헤더를 대신 읽는다.
 * 운영(app.security.enabled=true)에서는 이 헤더를 무시하고 JWT만 신뢰한다.
 */
@Component
public class CurrentUser {

    private static final String DEV_HEADER = "X-User-Id";

    private final boolean securityEnabled;

    public CurrentUser(@Value("${app.security.enabled:false}") boolean securityEnabled) {
        this.securityEnabled = securityEnabled;
    }

    public UUID id() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.getPrincipal() instanceof Jwt jwt && jwt.getSubject() != null) {
            return UUID.fromString(jwt.getSubject());
        }
        if (!securityEnabled) {
            String header = devHeader();
            if (header != null && !header.isBlank()) {
                return UUID.fromString(header.trim());
            }
        }
        throw new UnauthenticatedException("로그인이 필요합니다");
    }

    private String devHeader() {
        RequestAttributes attrs = RequestContextHolder.getRequestAttributes();
        if (attrs instanceof ServletRequestAttributes servlet) {
            return servlet.getRequest().getHeader(DEV_HEADER);
        }
        return null;
    }
}

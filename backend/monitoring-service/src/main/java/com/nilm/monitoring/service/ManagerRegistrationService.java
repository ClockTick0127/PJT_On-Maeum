package com.nilm.monitoring.service;

import com.nilm.monitoring.domain.Manager;
import com.nilm.monitoring.dto.kafka.ManagerRegisteredMessage;
import com.nilm.monitoring.repository.ManagerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 가입한 복지사를 담당자 명단에 올린다.
 *
 * <p>가입은 device_db에서 일어나고 담당자 명단은 monitoring_db에 있다. 두 DB를 FK로
 * 묶을 수 없으므로 이벤트로 이어 붙이고, 같은 이벤트를 두 번 받아도 한 명이 되도록
 * auth_sub로 판단한다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ManagerRegistrationService {

    private final ManagerRepository managers;

    @Transactional
    public void receive(ManagerRegisteredMessage message) {
        if (message.authSub() == null || message.authSub().isBlank()) {
            log.warn("authSub 없는 담당자 가입 무시: eventId={}", message.eventId());
            return;
        }
        if (message.name() == null || message.name().isBlank()
                || message.organization() == null || message.organization().isBlank()) {
            log.warn("이름·소속이 비어 담당자로 등록할 수 없음: authSub={}, eventId={}",
                    message.authSub(), message.eventId());
            return;
        }

        // 재전송이면 이미 있다. 가입 이후의 프로필 수정을 덮어쓰지 않도록 건드리지 않는다.
        if (managers.findByAuthSub(message.authSub()).isPresent()) {
            log.debug("이미 등록된 담당자: authSub={}, eventId={}",
                    message.authSub(), message.eventId());
            return;
        }

        managers.save(new Manager(
                message.authSub(),
                message.name(),
                message.organization(),
                message.phone(),
                usableEmail(message)));

        log.info("담당자 등록: authSub={}, organization={}, eventId={}",
                message.authSub(), message.organization(), message.eventId());
    }

    /**
     * 이메일은 담당자끼리 겹칠 수 없다. 남의 이메일과 부딪히면 그 값만 비우고 등록한다.
     * 이메일 하나 때문에 담당자가 아예 만들어지지 않으면 대상자 등록까지 막힌다.
     */
    private String usableEmail(ManagerRegisteredMessage message) {
        String email = message.email();
        if (email == null || email.isBlank()) {
            return null;
        }
        if (managers.existsByEmailIgnoreCase(email)) {
            log.warn("이미 쓰이는 이메일이라 비우고 등록: authSub={}, email={}",
                    message.authSub(), email);
            return null;
        }
        return email;
    }
}

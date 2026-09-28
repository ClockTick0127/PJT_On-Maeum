package com.nilm.monitoring.service;

import com.nilm.monitoring.domain.Manager;
import com.nilm.monitoring.domain.Subject;
import com.nilm.monitoring.dto.SubjectCreateRequest;
import com.nilm.monitoring.repository.ManagerRepository;
import com.nilm.monitoring.repository.SubjectRepository;
import lombok.RequiredArgsConstructor;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDate;
import java.time.ZoneId;
@Service
@RequiredArgsConstructor
public class SubjectRegistrationService {

    private static final ZoneId SEOUL =
            ZoneId.of("Asia/Seoul");

    private static final String HOUSEHOLD_UNIQUE_CONSTRAINT =
            "uq_subjects_household_id";

    private final ManagerRepository managers;
    private final SubjectRepository subjects;

    @Transactional
    public void register(
            String authSub,
            SubjectCreateRequest request
    ) {
        if (authSub == null || authSub.isBlank()) {
            throw new ResponseStatusException(
                    HttpStatus.UNAUTHORIZED,
                    "로그인이 필요합니다."
            );
        }

        // 담당자로 등록된 계정만 대상자를 생성할 수 있다.
        Manager manager = managers.findByAuthSub(authSub)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.FORBIDDEN,
                        "등록된 담당자만 대상자를 등록할 수 있습니다."
                ));

        validateBirthDate(request.birthDate());

        // 먼저 조회하여 일반적인 중복 요청을 처리한다.
        if (subjects.existsByHouseholdId(
                request.householdId()
        )) {
            throw householdAlreadyLinked();
        }

        Subject subject = new Subject(
                request.householdId(),
                request.name(),
                request.birthDate(),
                request.phone(),
                request.address(),
                request.addressDetail(),
                request.managerMemo(),
                manager.getId()
        );

        try {
            // INSERT를 실행하여 DB 유일 제약 위반도 여기서 확인한다.
            subjects.saveAndFlush(subject);
        } catch (DataIntegrityViolationException e) {
            // 동시 요청으로 발생한 같은 가구 중복만 409로 변환한다.
            if (isHouseholdDuplicate(e)) {
                throw householdAlreadyLinked();
            }

            // 다른 무결성 오류까지 가구 중복으로 처리하지 않는다.
            throw e;
        }
    }

    private void validateBirthDate(LocalDate birthDate) {
        if (birthDate == null
                || birthDate.isAfter(LocalDate.now(SEOUL))) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "생년월일은 오늘 또는 과거 날짜여야 합니다."
            );
        }
    }

    private boolean isHouseholdDuplicate(Throwable error) {
        Throwable current = error;

        while (current != null) {
            if (current instanceof ConstraintViolationException e
                    && HOUSEHOLD_UNIQUE_CONSTRAINT.equals(
                    e.getConstraintName()
            )) {
                return true;
            }

            current = current.getCause();
        }

        return false;
    }

    private ResponseStatusException householdAlreadyLinked() {
        return new ResponseStatusException(
                HttpStatus.CONFLICT,
                "이미 대상자에게 연결된 가구입니다."
        );
    }
}
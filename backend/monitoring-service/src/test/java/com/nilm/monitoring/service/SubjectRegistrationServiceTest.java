package com.nilm.monitoring.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.nilm.monitoring.domain.Manager;
import com.nilm.monitoring.dto.SubjectCreateRequest;
import com.nilm.monitoring.repository.ManagerRepository;
import com.nilm.monitoring.repository.SubjectRepository;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Optional;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

@ExtendWith(MockitoExtension.class)
class SubjectRegistrationServiceTest {

    @Mock
    ManagerRepository managers;

    @Mock
    SubjectRepository subjects;

    @InjectMocks
    SubjectRegistrationService service;

    private SubjectCreateRequest request;

    @BeforeEach
    void setUp() {
        Manager manager = mock(Manager.class);
        when(manager.getId()).thenReturn(7L);
        when(managers.findByAuthSub("manager-sub")).thenReturn(Optional.of(manager));
        request = new SubjectCreateRequest(
                "홍길동",
                LocalDate.of(1950, 1, 2),
                "01012345678",
                "서울특별시 중구",
                null,
                "H001",
                null
        );
    }

    @Test
    void concurrentHouseholdUniqueViolationIsConvertedToConflict() {
        when(subjects.existsByHouseholdId("H001")).thenReturn(false);
        when(subjects.saveAndFlush(any())).thenThrow(integrityError(
                "uq_subjects_household_id"
        ));

        assertThatThrownBy(() -> service.register("manager-sub", request))
                .isInstanceOfSatisfying(ResponseStatusException.class, error -> {
                    assertThat(error.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
                    assertThat(error.getReason())
                            .isEqualTo("이미 대상자에게 연결된 가구입니다.");
                });
    }

    @Test
    void unrelatedIntegrityViolationIsNotMisreportedAsHouseholdConflict() {
        DataIntegrityViolationException error = integrityError("another_constraint");
        when(subjects.existsByHouseholdId("H001")).thenReturn(false);
        when(subjects.saveAndFlush(any())).thenThrow(error);

        assertThatThrownBy(() -> service.register("manager-sub", request))
                .isSameAs(error);
    }

    private DataIntegrityViolationException integrityError(String constraintName) {
        ConstraintViolationException cause = new ConstraintViolationException(
                "constraint violation",
                new SQLException("duplicate"),
                constraintName
        );
        return new DataIntegrityViolationException("insert failed", cause);
    }
}

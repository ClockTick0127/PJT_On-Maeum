package com.nilm.monitoring.service;

import com.nilm.monitoring.domain.Subject;
import com.nilm.monitoring.dto.MyDashboardResponse;
import com.nilm.monitoring.repository.ManagerRepository;
import com.nilm.monitoring.repository.SubjectRepository;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
@RequiredArgsConstructor
public class MyDashboardService {

    private final SubjectRepository subjects;
    private final ManagerRepository managers;

    @Transactional(readOnly = true)
    public MyDashboardResponse getMyDashboard(String subjectAuthSub) {
        if (subjectAuthSub == null || subjectAuthSub.isBlank()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "로그인이 필요합니다.");
        }

        Subject subject = subjects.findByAuthSub(subjectAuthSub)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        "대상자 정보를 찾을 수 없습니다."
                ));

        MyDashboardResponse.ManagerSummary manager = subject.getManagerId() == null
                ? null
                : managers.findById(subject.getManagerId())
                        .map(value -> new MyDashboardResponse.ManagerSummary(
                                value.getName(),
                                value.getPhone()
                        ))
                        .orElse(null);

        // 캐시 플래그가 아니라 외출 구간에서 현재 시각으로 계산한다.
        // 예약만 걸려 있고 아직 시작하지 않은 상태를 구분해야 하기 때문이다.
        MyDashboardResponse.AwayMode awayMode = MyDashboardResponse.AwayMode.from(
                subject,
                OffsetDateTime.now(ZoneOffset.UTC)
        );

        return new MyDashboardResponse(
                subject.getId().toString(),
                subject.getName(),
                awayMode,
                manager
        );
    }
}

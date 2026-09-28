package com.nilm.device.api;

import com.nilm.device.api.dto.MemberDtos;
import com.nilm.device.security.CurrentUser;
import com.nilm.device.service.MembershipService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "Household Member", description = "가구 멤버십 — 접근 권한과 알림 수신자의 원본")
@RestController
@RequestMapping("/api/devices/households/{houseId}/members")
public class HouseholdMemberController {

    private final MembershipService membershipService;
    private final CurrentUser currentUser;

    public HouseholdMemberController(MembershipService membershipService, CurrentUser currentUser) {
        this.membershipService = membershipService;
        this.currentUser = currentUser;
    }

    @Operation(summary = "가구 멤버 목록",
            description = "PRIMARY 우선 정렬 — 알림 발송 순서 기준. 모니터링의 수신자 조회 진입점.")
    @GetMapping
    public List<MemberDtos.Response> list(@PathVariable String houseId) {
        return membershipService.list(houseId, currentUser.id());
    }

    @Operation(summary = "멤버 정보 수정", description = "관계·알림 순서·연락처·수신 여부 부분 수정 (null 필드는 유지).")
    @PatchMapping("/{userId}")
    public MemberDtos.Response update(@PathVariable String houseId,
                                      @PathVariable UUID userId,
                                      @Valid @RequestBody MemberDtos.UpdateRequest request) {
        return membershipService.update(houseId, userId, request, currentUser.id());
    }

    @Operation(summary = "멤버 접근 해제", description = "마지막 멤버는 해제할 수 없다(가구 고아화 방지).")
    @DeleteMapping("/{userId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void remove(@PathVariable String houseId, @PathVariable UUID userId) {
        membershipService.remove(houseId, userId, currentUser.id());
    }
}

package com.nilm.device.api;

import com.nilm.device.api.dto.InviteDtos;
import com.nilm.device.api.dto.MemberDtos;
import com.nilm.device.security.CurrentUser;
import com.nilm.device.service.InviteService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "Invite", description = "가구 초대 코드 — 로그인 수단이 아니라 접근 신청권")
@RestController
@RequestMapping("/api/devices")
public class InviteController {

    private final InviteService inviteService;
    private final CurrentUser currentUser;

    public InviteController(InviteService inviteService, CurrentUser currentUser) {
        this.inviteService = inviteService;
        this.currentUser = currentUser;
    }

    @Operation(summary = "초대 코드 발급",
            description = "해당 가구의 멤버만 발급할 수 있다. 1회용이며 기본 72시간 후 만료된다.")
    @PostMapping("/households/{houseId}/invites")
    @ResponseStatus(HttpStatus.CREATED)
    public InviteDtos.Response create(@PathVariable String houseId,
                                      @Valid @RequestBody InviteDtos.CreateRequest request) {
        return inviteService.create(houseId, request, currentUser.id());
    }

    @Operation(summary = "가구의 초대 코드 목록", description = "발급·사용·회수 이력 확인용.")
    @GetMapping("/households/{houseId}/invites")
    public List<InviteDtos.Response> list(@PathVariable String houseId) {
        return inviteService.list(houseId, currentUser.id());
    }

    @Operation(summary = "초대 수락",
            description = "로그인한 계정에 가구 멤버십을 만든다. 코드는 즉시 소비되어 재사용할 수 없다.")
    @PostMapping("/invites/{code}/accept")
    @ResponseStatus(HttpStatus.CREATED)
    public MemberDtos.Response accept(@PathVariable String code,
                                      @Valid @RequestBody InviteDtos.AcceptRequest request) {
        return inviteService.accept(code, request, currentUser.id());
    }

    @Operation(summary = "초대 코드 회수", description = "아직 사용되지 않은 코드를 무효화한다.")
    @DeleteMapping("/invites/{code}")
    public InviteDtos.Response revoke(@PathVariable String code) {
        return inviteService.revoke(code, currentUser.id());
    }
}

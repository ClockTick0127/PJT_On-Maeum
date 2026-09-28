package com.nilm.device.api;

import com.nilm.device.api.dto.HouseholdDtos;
import com.nilm.device.security.CurrentUser;
import com.nilm.device.service.HouseholdService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "Household", description = "가구 등록·조회")
@RestController
@RequestMapping("/api/devices/households")
public class HouseholdController {

    private final HouseholdService householdService;
    private final CurrentUser currentUser;

    public HouseholdController(HouseholdService householdService, CurrentUser currentUser) {
        this.householdService = householdService;
        this.currentUser = currentUser;
    }

    @Operation(summary = "가구 등록",
            description = "생성자가 첫 멤버(PRIMARY)로 함께 등록된다. "
                    + "기관 소속 계정이면 STAFF, 아니면 SELF 관계로 연결된다.")
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public HouseholdDtos.Response create(@Valid @RequestBody HouseholdDtos.CreateRequest request) {
        return householdService.create(request, currentUser.id());
    }

    @Operation(summary = "가구 목록 조회(관리용)",
            description = "내 가구 목록은 GET /api/auth/me 를 사용한다.")
    @GetMapping
    public List<HouseholdDtos.Response> list() {
        return householdService.list();
    }

    @Operation(summary = "가구 단건 조회", description = "해당 가구의 멤버만 조회할 수 있다.")
    @GetMapping("/{houseId}")
    public HouseholdDtos.Response get(@PathVariable String houseId) {
        return householdService.get(houseId, currentUser.id());
    }
}

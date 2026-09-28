package com.nilm.monitoring.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.time.OffsetDateTime;

public record NotificationResponseRequest (

    @NotNull
    @Pattern(regexp = "yes|no")
    String answer,

    @NotNull
    @Pattern(regexp = "user")
    String source,

    @NotNull
    OffsetDateTime respondedAt
){}

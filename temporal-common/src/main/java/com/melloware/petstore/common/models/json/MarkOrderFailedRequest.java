package com.melloware.petstore.common.models.json;

import java.util.UUID;

import com.melloware.petstore.common.models.enums.OrderFailureReason;

import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;

/**
 *
 
 */
@Builder
@Getter
@ToString
@Jacksonized
public class MarkOrderFailedRequest {
    
    @NotNull
    private final UUID transactionId;
    
    // Might not have it at time of error
    private final String orderNumber;
    
    @NotNull
    private final OrderFailureReason reason;
}
package com.banking.accountservice.dto;

import com.banking.accountservice.entity.AccountType;
import jakarta.persistence.Column;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CreateAccountRequest {

    @NotBlank(message = "Accounts holder name is required")
    private String accountHolderName;

    @NotBlank(message = "email is required")
    @Email(message = "Invalid emial format")
    private String email;

    @NotBlank(message = "phone is required")
    private String phone;

    @NotNull(message = "Account type is required")
    private AccountType accountType;

    @NotNull(message = "Initial deposit is required")
    @Positive(message = "Initial deposit mst be positive")
    private BigDecimal initialDeposit;
}

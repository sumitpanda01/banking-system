package com.banking.accountservice.controller;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.service.AccountService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;

@RestController
@RequestMapping("/api/v1/accounts")
@Slf4j
@RequiredArgsConstructor
public class AccountController {

    private final AccountService accountService;

    /**
     * create account
     * get account details
     * get account data
     * block account
     * deduct balance credit balance, credit receiver and credit sender, this is saga step no 4
     *
     * step no 1 SAGA
     *
     *
     */

    @PostMapping
    public ResponseEntity<AccountResponse> createAccount(
            @Valid @RequestBody CreateAccountRequest request
            ){

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(accountService.createAccount(request));
    }

    @GetMapping("/{accountNumber}")
    public ResponseEntity<AccountResponse> getAccount(
            @PathVariable String accountNumber
    ){
        return ResponseEntity.ok(accountService.getAccount(accountNumber));
    }

    //for get the balance of the user
    @GetMapping("/{accountNumber}/balance")
    public ResponseEntity<BigDecimal> getBalance(
            @PathVariable String accountNumber
    ){
        return ResponseEntity.ok(accountService.getBalance(accountNumber));
    }

    //for block the account
    @PutMapping("{accountNumber}/block")
    public ResponseEntity<String> blockAccount(
            @PathVariable String accountNumber
    ){
        accountService.blockAccount(accountNumber);
        return ResponseEntity.ok("Account Blocked Successfully")
    }

    //saga step no 1 - deduct balance also called by transaction service when transfer is initiated.
    public ResponseEntity<String> deductBalance(
            @PathVariable String accountNumber,
            @RequestParam BigDecimal amount
    ){
        accountService.deductBalance(accountNumber,amount);
        return ResponseEntity.ok("Balance deducted successfully");
    }


}

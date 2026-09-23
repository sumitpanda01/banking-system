package com.banking.transactionservice.entity;

/**
 * transation life cycle flow:-
 * PENDING -> PROCESSING -> COMPLETED (clean process transactions) i.e. money credit to receiver
 *
 * PENDING -> PROCESSING -> PENDING_VERIFICATION (suspicious detected)
 *                              -> COMPELTED (verified)
 *                               -> FLAGGED (saga refund)
 *                       -> FAILED
 *                       -> FLAGGED
 */
public enum TransactionStatus {
    PENDING,
    PROCESSING,
    PENDING_VERIFICATION,
    COMPLETED,
    FAILED,
    FLAGGED
}

package com.banking.transactionservice.service;

import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionEventConsumer {


    private final TransactionRepository transactionRepository;
    private final RedisTemplate<String, String> redisTemplate;
    private final TransactionService transactionService;
    private static final long OTP_EXPIRY_MINUTES =5;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TRANSACTION_OTP_GENERATED_TOPIC = "transaction.otp.generated";
    /**
     * consumer verification.required
     * Generate OTP and ask user to verify
     * @param payload
     */

    @KafkaListener(topics = "verification.required")
    public void consumerVerificationRequired(
            @Payload Map<String, Object> payload
            ){

        try{
            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            log.info("Verification required - transaction : {} reason : {}",
                    transactionId, reason);

            Transaction transaction = transactionRepository.findById(transactionId)
                    .orElseThrow(() -> new RuntimeException("Transaction not found" + transactionId));

            //here we have to add idempotency guard because :-
            //in order to prevent two time credit to my receiver or to my sender

            //here are verifying user only if the transaction.Status is processing
            //because if the status is not processing this mean that either the
            //transaction is completed or the transaction is flagged

            if(transaction.getStatus() != TransactionStatus.PROCESSING){
                log.warn("Transaction {} not PROCESSING - skipping", transactionId);
                return;
            }

            //Generate 6 digit OTP
            String otp = String.format("%06d", (int) (Math.random() * 900000) + 100000);

            //store OTP in redis- expires in 5 minutes
            String otpKey = "verification:otp" + transactionId;
            redisTemplate.opsForValue().set(otpKey, otp, OTP_EXPIRY_MINUTES, TimeUnit.MINUTES);

            //update status
            transaction.setStatus(TransactionStatus.PENDING_VERIFICATION);
            transactionRepository.save(transaction);

            log.info("OTP generated for transaction : {} expires in {} min",
                    transaction, OTP_EXPIRY_MINUTES);

            //Notify user:- by sending OTP to the user via sms or via mail
            Map<String, Object> otpEvent = new HashMap<>();
            otpEvent.put("transactionId", transactionId);
            otpEvent.put("accountNumber", accountNumber);
            otpEvent.put("reason", reason);
            otpEvent.put("otp",otp);
            otpEvent.put("amount", payload.get("amount"));

            kafkaTemplate.send(TRANSACTION_OTP_GENERATED_TOPIC, transactionId, otpEvent);

        } catch (Exception e) {
            log.error("Error handling verification required : {}", e.getMessage());
        }
    }


    @KafkaListener(topics = "fraud.check.clean")
    public void consumeFraudCheckCleanResult(
            @Payload Map<String, Object> payload
    ){
        try{
            String transactionId = (String) payload.get("transactionId");
            transactionService.processCleanResult(transactionId);
        } catch (Exception e) {
            log.error("Error processing fraud check result: {}", e.getMessage());
        }
    }
}

package com.example.notificationservice.consumer;

import com.example.notificationservice.model.EmployeeMessage;
import com.example.notificationservice.validation.EmailValidationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class EmployeeCreatedConsumer {

    private static final Logger log = LoggerFactory.getLogger(EmployeeCreatedConsumer.class);

    private final EmailValidationService emailValidationService;

    public EmployeeCreatedConsumer(
            EmailValidationService emailValidationService) {
        this.emailValidationService = emailValidationService;
    }

    @KafkaListener(topics = "employee-created", groupId = "notification-group")
    public void consume(EmployeeMessage employee) {

        String email = employee.getEmail();

        if (emailValidationService.isValid(email)) {
            log.info("Notification sent to: {}", email);
        } else {
            log.error(
                    "Invalid email '{}'. Notification skipped.",
                    email);
        }
    }
}
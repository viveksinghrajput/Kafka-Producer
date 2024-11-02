package com.kafka.controller;

import com.kafka.dto.Customer;
import com.kafka.service.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer-app")
public class KafkaProducerController {
    @Autowired
    public KafkaProducerService publisher;

    @GetMapping("/publish/{message}")
    public ResponseEntity<?> publishMessage(@PathVariable String message) {
        try {
            for (int i = 0; i < 10000; i++) {
                publisher.sendMessageToTopic(message + "-" + i);
            }
            return ResponseEntity.ok("Message publish successfully ......");
        } catch (Exception ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }

    }

    @PostMapping("/publish")
    public ResponseEntity<?> sendEvents(@RequestBody Customer customer) {
        try {
            publisher.sendEventsToTopic(customer);
            return ResponseEntity.ok("Message publish successfully ......");
        } catch (Exception ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}

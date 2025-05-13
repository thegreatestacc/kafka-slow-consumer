package com.example.kafkaconsumer;

import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Vladimir Solovyov
 * @project kafka-slow-consumer-demo
 * @date on 12/05/2025
 */

@Component
public class KafkaMessageProducer implements CommandLineRunner {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ExecutorService executor = Executors.newFixedThreadPool(3);

    public KafkaMessageProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    @Override
    public void run(String... args) {
        for (int i = 1; i <= 3; i++) {
            int threadId = i;
            executor.submit(() -> {
                for (int j = 1; j <= 5; j++) {
                    String message = "message" + threadId + "-" + j;
                    kafkaTemplate.send("demo-topic", message);
                    System.out.printf("Producer %d sent: %s%n", threadId, message);
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }
    }
}
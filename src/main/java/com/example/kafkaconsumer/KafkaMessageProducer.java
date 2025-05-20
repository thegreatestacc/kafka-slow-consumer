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

    public KafkaMessageProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // переделал на однопоточный продюсер
    @Override
    public void run(String... args) {
        Executors.newFixedThreadPool(3).submit(() -> {
            int i = 0;
            while (true) {
                String message = "message3-" + i++;
                kafkaTemplate.send("demo-topic", message);
                System.out.println("Producer sent: " + message);
                try {
                    Thread.sleep(5_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    /*private final KafkaTemplate<String, String> kafkaTemplate;
    private final ExecutorService executor = Executors.newFixedThreadPool(3);

    public KafkaMessageProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    @Override
    public void run(String... args) {
        for (int i = 1; i <= 3; i++) {
            int threadId = i;
            executor.submit(() -> {
                int counter = 1;
                while (true) {
                    String message = "message" + threadId + "-" + counter++;
                    kafkaTemplate.send("demo-topic", message);
                    System.out.printf("Producer %d sent: %s%n", threadId, message);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });
        }
    }*/
}
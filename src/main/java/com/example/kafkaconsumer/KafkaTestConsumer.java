package com.example.kafkaconsumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class KafkaTestConsumer {

    private final ExecutorService executorService = Executors.newFixedThreadPool(3);

    @KafkaListener(
            topics = "demo-topic",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(String message, Acknowledgment ack) {
        String threadName = Thread.currentThread().getName();
        System.out.println("Получено: " + message + " | Поток: " + threadName);

        executorService.submit(() -> {
            try {
                if (message.contains("3")) {
                    Thread.sleep(10000);
                } else {
                    Thread.sleep(1000);
                }

                ack.acknowledge();
                System.out.println("Коммит выполнен: " + message + " | Поток: " + threadName);

            } catch (Exception e) {
                System.err.println("Ошибка при обработке: " + message + " | " + e.getMessage());
            }
        });
    }

    //без ExecutorService уходит в бесконечную перебалансировку консьюмеров

    /*@KafkaListener(topics = "demo-topic", containerFactory = "kafkaListenerContainerFactory")
    public void listen(String message, Acknowledgment ack) {
        String threadName = Thread.currentThread().getName();
        System.out.println("Получено: " + message + " | Поток: " + threadName);

        try {
            if (message.contains("3")) {
                Thread.sleep(10000);
            } else {
                Thread.sleep(1000);
                System.out.println("Коммит выполнен: " + message + " | Поток: " + threadName);
            }
            ack.acknowledge();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }*/
}
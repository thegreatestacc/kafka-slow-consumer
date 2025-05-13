package com.example.kafkaconsumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class KafkaTestConsumer {

    @KafkaListener(topics = "demo-topic", containerFactory = "kafkaListenerContainerFactory")
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
    }
}
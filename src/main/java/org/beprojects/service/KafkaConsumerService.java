package org.beprojects.service;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.beprojects.deserializer.SellDeserializer;
import org.beprojects.model.Sell;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class KafkaConsumerService {
    private static final String TOPIC_NAME = "ticket-sell";
    private static final String BROKER_ADDRESS = "localhost:9092";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_ADDRESS);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SellDeserializer.class.getName());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo-processamento");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, Sell> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(TOPIC_NAME));
            while (true) {
                ConsumerRecords<String, Sell> poll = consumer.poll(Duration.ofSeconds(500));
                poll.forEach(record -> {
                    Sell sellRecoverd = record.value();

                    checkSale();

                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.println(sellRecoverd);
                });
            }
        }

    }

    private static void checkSale() {
        if (new Random().nextBoolean()){
            System.out.println("Sell APPROVED");
        } else {
            System.out.println("Sell NOT APPROVED");
        }
    }

}
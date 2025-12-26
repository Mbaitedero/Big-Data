package org.example.Consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "simple-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Lire depuis le début

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("wikipedia.recentchange"));

            System.out.println("🔍 Démarrage du consommateur simple...");
            System.out.println("Lecture des 5 premiers messages...\n");

            int messageCount = 0;
            while (messageCount < 5) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

               if (records.isEmpty()) {
                   System.out.println("Aucun nouveau message...");
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("📨 MESSAGE " + (messageCount + 1));
                    System.out.println("   Clé: " + (record.key() != null ? record.key() : "NULL"));
                    System.out.println("   Valeur: " + record.value());
                    System.out.println("   Partition: " + record.partition());
                    System.out.println("   Offset: " + record.offset());
                    System.out.println("   Timestamp: " +
                            java.time.Instant.ofEpochMilli(record.timestamp()));
                    System.out.println("   ---");

                    messageCount++;
                    if (messageCount >= 5) break;
                }
            }

            System.out.println("✅ 5 messages lus. Arrêt du consommateur simple.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
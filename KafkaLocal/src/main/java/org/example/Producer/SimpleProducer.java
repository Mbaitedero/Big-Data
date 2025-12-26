package org.example.Producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Optimisations pour un débit plus important
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("🚀 Démarrage du producteur sans clé...");

            for (int i = 1; i <= 20; i++) {
                String message = "Message sans clé #" + i + " - " + java.time.LocalTime.now();

                // Envoi sans clé (la clé est null)
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        "wikipedia.recentchange",
                        message
                );

                producer.send(record);
                System.out.println("✅ Message envoyé: " + message);

                Thread.sleep(500); // Pause de 500ms entre les messages
            }

            System.out.println("✨ Tous les messages ont été envoyés !");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
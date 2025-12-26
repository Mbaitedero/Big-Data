package org.example.Producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import java.util.Properties;
import java.util.concurrent.Future;

public class KeyedProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all"); // Attendre l'acknowledgement de tous les replicas
        props.put("retries", 3);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("🚀 Démarrage du producteur avec clé...");

            // Types d'utilisateurs pour démontrer le partitionnement par clé
            String[] userTypes = {"admin", "moderator", "user", "guest"};
            String[] actions = {"created", "updated", "deleted", "viewed"};

            for (int i = 1; i <= 10; i++) {
                String userType = userTypes[i % userTypes.length];
                String action = actions[i % actions.length];
                String key = userType + "-" + (1000 + i);
                String value = String.format("User %s a %s la page Wikipedia #%d à %s",
                        userType, action, i, java.time.LocalTime.now());

                ProducerRecord<String, String> record = new ProducerRecord<>(
                        "wikipedia.recentchange",
                        key,  // Clé spécifiée - garantit le même partition
                        value
                );

                // Envoi avec callback pour confirmation
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            System.out.println("✅ Message avec clé '" + key + "' -> " +
                                    "Partition: " + metadata.partition() +
                                    ", Offset: " + metadata.offset());
                        } else {
                            System.err.println("❌ Erreur pour la clé '" + key + "': " + exception.getMessage());
                        }
                    }
                });

                Thread.sleep(300); // Pause plus courte pour plus de messages
            }

            // Attendre que tous les messages soient envoyés
            producer.flush();
            System.out.println("✨ Tous les messages avec clé ont été envoyés !");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
package org.example.Consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class RealTimeConsumer {
    private static volatile boolean running = true;

    public static void main(String[] args) {
        // Gestion propre de l'arrêt avec Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n🛑 Arrêt demandé...");
            running = false;
        }));

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "realtime-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        // Pour un traitement plus rapide
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "100");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("wikipedia.recentchange"));

            System.out.println("🌐 CONSOMMATEUR TEMPS RÉEL DÉMARRÉ");
            System.out.println("==================================");
            System.out.println("Topic: wikipedia.recentchange");
            System.out.println("Group: realtime-consumer-group");
            System.out.println("Appuyez sur Ctrl+C pour arrêter");
            System.out.println("==================================\n");

            int totalMessages = 0;

            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                if (records.isEmpty()) {
                    // Aucun message, on attend
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    totalMessages++;
                    processRealTimeMessage(record, totalMessages);
                }
            }

            System.out.println("\n📊 STATISTIQUES FINALES:");
            System.out.println("Total messages traités: " + totalMessages);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void processRealTimeMessage(ConsumerRecord<String, String> record, int count) {
        String keyInfo = record.key() != null ?
                "Clé: " + record.key() + " | " :
                "Sans clé | ";

        System.out.printf("📊 Message #%d | %sPartition: %d | Offset: %d%n",
                count, keyInfo, record.partition(), record.offset());
        System.out.println("   📝 Contenu: " + record.value());
        System.out.println("   ⏰ Temps: " +
                java.time.Instant.ofEpochMilli(record.timestamp())
                        .atZone(java.time.ZoneId.systemDefault())
                        .toLocalTime());
        System.out.println("   ---");

        // Ici vous pouvez ajouter votre logique de traitement :
        // - Stockage en base de données
        // - Analyse en temps réel
        // - Notification
        // - Agrégation de données
    }
}
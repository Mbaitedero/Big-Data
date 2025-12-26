package com.bigdata.producer;

import com.bigdata.model.LogMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LogsProducer {

    private static final String[] LOG_LEVELS = {"INFO", "DEBUG", "WARN", "ERROR"};
    private static final String[] SERVICES = {"auth-service", "user-service", "order-service",
            "payment-service", "inventory-service", "api-gateway"};
    private static final String[] MESSAGES = {
            "User login successful", "Database connection established",
            "Cache miss for key", "Request processing time exceeded threshold",
            "Memory usage above 80%", "Database connection timeout",
            "NullPointerException in user controller", "OutOfMemoryError detected",
            "Payment processing failed", "Inventory update completed"
    };

    private static final Random random = new Random();
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.registerModule(new JavaTimeModule());
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100); // Batch optimization
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        System.out.println("🚀 Démarrage du producteur de logs...");

        scheduler.scheduleAtFixedRate(() -> {
            try {
                // Générer 1-3 logs à chaque intervalle
                int logsCount = 1 + random.nextInt(3);
                for (int i = 0; i < logsCount; i++) {
                    LogMessage log = generateLog();
                    String message = mapper.writeValueAsString(log);

                    // Utiliser le service comme clé pour garantir l'ordre des logs par service
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>("logs-raw", log.getService(), message);

                    producer.send(record, (metadata, exception) -> {
                        if (exception == null) {
                            System.out.printf("📝 Log envoyé: [%s] %s -> Partition %d\n",
                                    log.getLevel(), log.getService(), metadata.partition());
                        } else {
                            System.err.println("❌ Erreur envoi log: " + exception.getMessage());
                        }
                    });
                }

            } catch (JsonProcessingException e) {
                System.err.println("Erreur sérialisation JSON: " + e.getMessage());
            }
        }, 0, 1, TimeUnit.SECONDS); // Logs toutes les secondes

        // Arrêt propre après 2 heures
        scheduler.schedule(() -> {
            System.out.println("🛑 Arrêt du producteur de logs...");
            scheduler.shutdown();
            producer.close();
        }, 2, TimeUnit.HOURS);
    }

    private static LogMessage generateLog() {
        String id = "LOG-" + System.currentTimeMillis() + "-" + random.nextInt(1000);
        String level = weightedRandomLevel();
        String service = SERVICES[random.nextInt(SERVICES.length)];
        String message = MESSAGES[random.nextInt(MESSAGES.length)];
        String thread = "thread-" + random.nextInt(10);
        String timestamp = java.time.Instant.now().toString(); // Utilisez String au lieu de LocalDateTime

        String stackTrace = null;
        if ("ERROR".equals(level) && random.nextBoolean()) {
            stackTrace = "java.lang.NullPointerException: null reference\n" +
                    "   at com.example.Service.method(Service.java:123)\n" +
                    "   at com.example.Controller.handle(Controller.java:45)";
        }

        return new LogMessage(id,
                level,
                message,
                service,
                thread,
                timestamp,
                stackTrace,
                null,
                null,
                null);
    }

    private static String weightedRandomLevel() {
        double rand = random.nextDouble();
        if (rand < 0.6) return "INFO";      // 60% INFO
        if (rand < 0.8) return "DEBUG";     // 20% DEBUG
        if (rand < 0.95) return "WARN";     // 15% WARN
        return "ERROR";                     // 5% ERROR
    }
}
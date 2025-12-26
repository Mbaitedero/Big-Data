package com.bigdata.streams;

import com.bigdata.model.LogMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

public class LogsProcessor {

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    // Méthode utilitaire pour parser les logs
    private static LogMessage parseLogMessage(String json) {
        try {
            return mapper.readValue(json, LogMessage.class);
        } catch (IOException e) {
            System.err.println("❌ Erreur parsing JSON: " + e.getMessage());
            System.err.println("📄 JSON problématique: " + json);
            return null;
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "logs-processor-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Stream des logs bruts
        KStream<String, String> rawLogs = builder.stream("logs-raw");

        // 1. Traitement principal : Enrichissement et transformation
        KStream<String, String> processedLogs = rawLogs
                .mapValues(value -> {
                    LogMessage log = parseLogMessage(value);
                    if (log == null) {
                        return value;
                    }

                    try {
                        // Créer un JSON enrichi
                        String enrichedJson = String.format(
                                "{\"id\":\"%s\",\"level\":\"%s\",\"message\":\"%s\",\"service\":\"%s\"," +
                                        "\"thread\":\"%s\",\"timestamp\":\"%s\",\"stackTrace\":%s,\"processing_timestamp\":\"%s\"}",
                                log.getId(),
                                log.getLevel(),
                                log.getMessage().replace("\"", "\\\""),
                                log.getService(),
                                log.getThread(),
                                log.getFormattedTimestamp(),
                                log.getStackTrace() != null ? "\"" + log.getStackTrace().replace("\"", "\\\"") + "\"" : "null",
                                java.time.Instant.now()
                        );
                        return enrichedJson;
                    } catch (Exception e) {
                        System.err.println("❌ Erreur enrichissement log: " + e.getMessage());
                        return value;
                    }
                });

        // Envoyer vers le topic des logs traités
        processedLogs.to("logs-processed");

        // 2. Détection des logs critiques pour les alertes
        KStream<String, String> criticalAlerts = rawLogs
                .filter((key, value) -> {
                    LogMessage log = parseLogMessage(value);
                    return log != null && (log.isCritical() || log.isErrorLevel());
                })
                .mapValues(value -> {
                    LogMessage log = parseLogMessage(value);
                    if (log == null) {
                        return "Erreur création alerte: " + value;
                    }

                    String alertMessage = String.format(
                            "🚨 ALERTE [%s] Service: %s | Message: %s | Thread: %s | Timestamp: %s",
                            log.getLevel(), log.getService(), log.getMessage(), log.getThread(), log.getFormattedTimestamp()
                    );

                    if (log.getStackTrace() != null) {
                        alertMessage += "\nStack Trace: " + log.getStackTrace();
                    }

                    return alertMessage;
                });

        criticalAlerts.to("logs-alerts");

        // 3. Statistiques des logs par niveau
        KTable<Windowed<String>, Long> logsByLevel = rawLogs
                .groupBy((key, value) -> {
                    LogMessage log = parseLogMessage(value);
                    return (log != null) ? log.getLevel() : "UNKNOWN";
                })
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                .count();

        logsByLevel.toStream()
                .map((windowedKey, count) -> {
                    String stats = String.format("Niveau: %s | Fenêtre: %s | Count: %d",
                            windowedKey.key(),
                            windowedKey.window().startTime().toString().substring(11, 19),
                            count);
                    return KeyValue.pair(windowedKey.key(), stats);
                })
                .to("logs-stats", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("🛑 Arrêt du Stream Processor...");
            streams.close();
        }));

        System.out.println("🚀 Démarrage du traitement des logs...");
        streams.start();
    }
}
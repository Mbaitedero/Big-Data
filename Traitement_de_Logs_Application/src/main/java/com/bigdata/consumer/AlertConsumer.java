package com.bigdata.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AlertConsumer {

    private static final ExecutorService notificationExecutor = Executors.newFixedThreadPool(3);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "alerts-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("logs-alerts"));

        System.out.println("👀 Consommateur d'alertes démarré - En attente d'alertes...");

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    String alertMessage = record.value();
                    String service = record.key();

                    System.out.println("\n🚨 ======= ALERTE CRITIQUE =======");
                    System.out.println(alertMessage);
                    System.out.println("📍 Service: " + service);
                    System.out.println("📊 Partition: " + record.partition() + " | Offset: " + record.offset());
                    System.out.println("================================\n");

                    // Traitement asynchrone des notifications
                    notificationExecutor.submit(() -> processAlert(alertMessage, service));
                }
            }
        } finally {
            notificationExecutor.shutdown();
            consumer.close();
        }
    }

    private static void processAlert(String alertMessage, String service) {
        try {
            // 1. Envoi d'email (simulé)
            sendEmailAlert(alertMessage, service);

            // 2. Notification Slack (simulée)
            sendSlackNotification(alertMessage, service);

            // 3. Création de ticket (simulée)
            createSupportTicket(alertMessage, service);

            // 4. Log dans un fichier d'alertes
            logAlertToFile(alertMessage, service);

        } catch (Exception e) {
            System.err.println("❌ Erreur lors du traitement de l'alerte: " + e.getMessage());
        }
    }

    // 1. Simulation d'envoi d'email
    private static void sendEmailAlert(String alertMessage, String service) {
        String emailContent = String.format("""
            ===============================
            🚨 ALERTE SYSTÈME - %s
            ===============================
            Service: %s
            Heure: %s
            Message: %s

            Cette alerte nécessite votre attention immédiate.
            ===============================
            """, service, service, LocalDateTime.now(), alertMessage);

        // Simulation d'envoi d'email
        System.out.println("📧 EMAIL ENVOYÉ À: admin@company.com");
        System.out.println("📧 SUJET: 🚨 Alerte Critique - " + service);
        System.out.println("📧 CONTENU:\n" + emailContent);

        // Ici vous intégreriez une vraie bibliothèque d'email comme:
        // - JavaMail API
        // - Spring Boot Mail
        // - Amazon SES
        // - SendGrid
    }

    // 2. Simulation de notification Slack
    private static void sendSlackNotification(String alertMessage, String service) {
        String slackMessage = String.format("""
            {
                "channel": "#system-alerts",
                "username": "Kafka Alert Bot",
                "text": "🚨 *Alerte Critique* - %s",
                "attachments": [
                    {
                        "color": "danger",
                        "fields": [
                            {
                                "title": "Service",
                                "value": "%s",
                                "short": true
                            },
                            {
                                "title": "Heure",
                                "value": "%s",
                                "short": true
                            },
                            {
                                "title": "Message",
                                "value": "%s"
                            }
                        ]
                    }
                ]
            }
            """, service, service, LocalDateTime.now(), alertMessage);

        // Simulation d'envoi Slack
        System.out.println("💬 SLACK NOTIFICATION ENVOYÉE");
        System.out.println("💬 Channel: #system-alerts");
        System.out.println("💬 Message: " + slackMessage.replace("\n", " ").substring(0, 100) + "...");

        // Intégrations possibles:
        // - Webhook Slack
        // - Bibliothèque Slack SDK
        // - Bot personnalisé
    }

    // 3. Simulation de création de ticket
    private static void createSupportTicket(String alertMessage, String service) {
        String ticketDescription = String.format("""
            **Alerte Système Automatique**

            **Service:** %s
            **Sévérité:** CRITIQUE
            **Description:** %s
            **Détection:** Système Kafka Monitoring
            **Timestamp:** %s

            **Actions Requises:**
            - [ ] Investigation immédiate
            - [ ] Correction du problème
            - [ ] Documentation de l'incident
            """, service, alertMessage, LocalDateTime.now());

        // Simulation de création de ticket
        String ticketId = "TICKET-" + System.currentTimeMillis();
        System.out.println("🎫 TICKET CRÉÉ: " + ticketId);
        System.out.println("🎫 Système: Jira/ServiceNow");
        System.out.println("🎫 Description: " + ticketDescription.replace("\n", " ").substring(0, 100) + "...");

        // Intégrations possibles:
        // - API Jira
        // - API ServiceNow
        // - API Zendesk
        // - Webhook personnalisé
    }

    // 4. Log des alertes dans un fichier
    private static void logAlertToFile(String alertMessage, String service) {
        String logEntry = String.format("[%s] ALERTE - Service: %s - Message: %s\n",
                LocalDateTime.now(), service, alertMessage);

        // Simulation d'écriture dans un fichier
        System.out.println("📝 ALERTE LOGGÉE DANS: /var/log/kafka-alerts.log");
        System.out.println("📝 Entrée: " + logEntry.trim());

        // Implémentation réelle:
        /*
        try (FileWriter fw = new FileWriter("/var/log/kafka-alerts.log", true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            out.println(logEntry);
        } catch (IOException e) {
            System.err.println("Erreur écriture fichier: " + e.getMessage());
        }
        */
    }
}
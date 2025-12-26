package org.example.Producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class FirstProducer_Callback {

    private static final Logger logger = LoggerFactory.getLogger(FirstProducer_Callback.class);

    public void sendMessagesCallback(){

        String bootstrapServers ="pkc-921jm.us-east-2.aws.confluent.cloud:9092";

        //Create Properties//Required connection configs for Kafka producer, consumer, and admin
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"pkc-921jm.us-east-2.aws.confluent.cloud:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule " +
                "required username='UG2GBYJ5HQP7HYSD' " +
                "password='cfltny2U3RMv2RsY+lnUBmvsGoApgkHpNdffg1x+CaSXveHXo2UYCCXLSFjNhUow';");

        properties.setProperty("sasl.mechanism","PLAIN");
        properties.setProperty("client.dns.lookup","use_all_dns_ips");
        properties.setProperty("session.timeout.ms","45000");
        properties.setProperty("acks","all");
        properties.setProperty("schema.registry.url","https://{{ SR_ENDPOINT }}");
        properties.setProperty("basic.auth.credentials.source","USER_INFO");
        properties.setProperty("basic.auth.user.info","{{ SR_API_KEY }}:{{ SR_API_SECRET }}");

        KafkaProducer<String, String> producer_2 = new KafkaProducer<>(properties);


        for (int i=0;i<2;i++) {
        //Create Record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("topic_2",
                    "Football_subject" + Integer.toString(i));
         //Send messages
            producer_2.send(record, new Callback() {
                public void onCompletion (RecordMetadata recordMetadata, Exception e) {
                    Logger logger = LoggerFactory.getLogger (FirstProducer_Callback.class);
            // executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        logger.info("Successfully received the details as: \n" +
                                "topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset" + recordMetadata.offset() + "\n" +
                                "Timestamp" + recordMetadata.timestamp());
                    } else {
                        logger.error("Can't produce, getting error", e);
                    }
                }
            });
        }
        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // S'assurer que tous les messages sont envoyés avant de quitter
        producer_2.flush();
        producer_2.close();

    }
}

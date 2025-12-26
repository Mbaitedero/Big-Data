package org.example.Producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class FirstProducer_Demo {
    public void sendMessage(){

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


        //Create Producer
        KafkaProducer<String, String> producer_3 = new KafkaProducer<String, String>(properties);
       //Create Record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("topic_2",
                "Bonjour tout  le  monde");
        //Send messages
        producer_3.send(record); //send data - asynchronous
        producer_3.flush(); //flush data - synchronous
        producer_3.close();




    }
}

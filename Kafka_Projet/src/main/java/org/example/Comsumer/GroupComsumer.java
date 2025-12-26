package org.example.Comsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class GroupComsumer {
    public   void  receiveByGroupMessage(){
        Properties properties = new Properties();

        String bootstrapServers ="pkc-921jm.us-east-2.aws.confluent.cloud:9092";
        //Create Consumer Configs
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-921jm.us-east-2.aws.confluent.cloud:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "First_Consumers_Group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //Réinitialisation automatique de l'offset. La réinitialisation automatique du décalage est réglée sur
        // ce qui signifie que lors de la première exécution de l'applicotion
        // nous lirons toutes les données historiques de notre topic.
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule " +
                "required username='UG2GBYJ5HQP7HYSD'" +
                "password='cfltny2U3RMv2RsY+lnUBmvsGoApgkHpNdffg1x+CaSXveHXo2UYCCXLSFjNhUow';");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("client.dns.lookup","use_all_dns_ips");
        properties.setProperty("session.timeout.ms", "45000");
        properties.setProperty("acks","all");
        properties.setProperty("schema.registry.url", "https://psrc-1ynovvj.us-east-2.aws.confluent.cloud");
        properties.setProperty("basic.auth.credentials.source", "USER_INFO");
        properties.setProperty("basic.auth.user.info","'UG2GBYJ5HQP7HYSD':'cfltny2U3RMv2RsY+lnUBmvsGoApgkHpNdffg1x+CaSXveHXo2UYCCXLSFjNhUow'");
        String topic = "topic_2";

        //Create Consumer
        KafkaConsumer<String, String> consumer_1 = new KafkaConsumer<String, String>(properties);
        // subscribe consumer to the topic(s)
        consumer_1.subscribe(Arrays.asList(topic));
        Logger logger= LoggerFactory.getLogger (GroupComsumer.class);
        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

       // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer_1.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // subscribe consumer to the topic(s)
            consumer_1.subscribe(Arrays.asList(topic));
            // poll for new data
            while (true) {
                ConsumerRecords<String, String> records = consumer_1.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key: " + record.key() + ", Value: " + record.value());
                    logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                }
            }

        } catch (WakeupException e) {
            logger.info("Wake up exception!");
            // we ignore this as this is an expected exception when closing a consumer
        } catch (Exception e) {
            logger.error("Unexpected exception", e);
        } finally {
            consumer_1.close(); // this will also commit the offsets
            logger.info("The consumer is now gracefully closed.");
        }

    }
}

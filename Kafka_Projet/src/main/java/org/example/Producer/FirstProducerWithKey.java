package org.example.Producer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class FirstProducerWithKey {
    private static final Logger logger = LoggerFactory.getLogger(FirstProducerWithKey.class);
    public  void sendMessageWithKey() throws ExecutionException, InterruptedException {
        String bootstrapServers ="pkc-921jm.us-east-2.aws.confluent.cloud:9092";
        //Create Properties//Required connection configs for Kafka producer, consumer, and admin
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"pkc-921jm.us-east-2.aws.confluent.cloud:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config",
                                "org.apache.kafka.common.security.plain.PlainLoginModule " +
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
        KafkaProducer<String, String> producer_1 = new KafkaProducer<String, String>(properties);
        List<String> Articles_types = Arrays.asList("Football", "Tennis", "Basketball");
        Random rand = new Random();
        for(int i=1; i<=4;i++){
            String Topic = "topic_2";
            String Key = null;
            String randomElement = Articles_types.get(rand.nextInt(Articles_types.size()));
            switch(randomElement) {
                case "Football":
                    Key = "id_0"; break;
                case "Tennis":
                    Key = "id_1"; break;
                case "Basketball":
                    Key = "id_2"; break;
            }

                String Value = i + "Article de " + randomElement;
                ProducerRecord<String, String> record=new ProducerRecord<String, String> (Topic, Key, Value);
                logger.info("Key---"+ Key);
                //Send messages
                producer_1.send(record, new Callback() {
                public void onCompletion (RecordMetadata recordMetadata, Exception e) {
                    if (e== null) {
                        logger.info("Successfully received the details as: \n" +
                                        "Topic:" + recordMetadata.topic() + "\n" +
                                        "Key:" + record.key() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset: "+ recordMetadata.offset() + "\n" +
                                        "Timestamp: "+ recordMetadata.timestamp());
                    }
                    else { logger.error("Can't produce, getting error",e);
                    }
                } }).get();//send synchronous data forcefully;
        }
    }
}

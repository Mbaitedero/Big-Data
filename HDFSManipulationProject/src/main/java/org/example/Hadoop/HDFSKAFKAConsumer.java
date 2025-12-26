package org.example.Hadoop;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class HDFSKAFKAConsumer {
    private HDFSConnexion connexion;
    private Properties prop;

    public HDFSKAFKAConsumer() throws IOException {
        connexion  =  new HDFSConnexion();
        prop = new Properties();

        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"test");
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
    }
    public HDFSConnexion getConnexion() {
        return connexion;
    }
    public Properties getProp() {
        return prop;
    }
    public void KafkaWriter(FileSystem fs, Properties properties, String topic, String hdfsPath) throws IOException {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        Path path = new Path(hdfsPath);

        FSDataOutputStream outputStream;
        if (fs.exists(path)) {
            outputStream = fs.append(path);
        } else {
            outputStream = fs.create(path);
        }
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
        System.out.println("Consuming from kafka and writing to hdfs.......;");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                writer.write(record.value());
                writer.newLine();
                System.out.println(record.value() +"\n");
            }
            writer.flush();
        }
    }
    public static void main(String[] args) throws IOException {
        HDFSKAFKAConsumer consumer = new HDFSKAFKAConsumer();
        consumer.KafkaWriter(consumer.getConnexion().getFS(),
                consumer.getProp(),"sports","/user/folder_1/kafkalogs");
    }
}

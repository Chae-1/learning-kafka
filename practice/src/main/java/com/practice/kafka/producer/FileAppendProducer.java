package com.practice.kafka.producer;

import com.practice.kafka.event.EventHandler;
import com.practice.kafka.event.FileEventHandler;
import com.practice.kafka.event.FileEventSource;
import java.io.File;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileAppendProducer {

    private static final String SERVER_IP = "192.168.56.101:9092";
    private static final Logger log = LoggerFactory.getLogger(FileAppendProducer.class);

    public static void main(String[] args) throws InterruptedException {
        String topicName = "file-topic";

        //KafkaProducer 객체
        Properties props = new Properties();

        //bootstrap-server, key.serializer.class, value.serializer.class
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_IP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //kafkaProducer object operation
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            boolean sync = false;

            String filePath = "C:\\KafkaProj_01\\practice\\src\\main\\resources\\pizza_append.txt";
            File file = new File(filePath);
            EventHandler eventHandler = new FileEventHandler(producer, topicName, sync);

            FileEventSource fileEventSource = new FileEventSource(file, 20000, eventHandler);
            Thread thread = new Thread(fileEventSource);
            thread.start();
            thread.join();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}

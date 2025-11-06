package com.practice.kafka.producer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileProducer {
    public static final String SERVER_IP = "192.168.56.101:9092";
    public static final Logger logger = LoggerFactory.getLogger(
            FileProducer.class
    );


    public static void main(String[] args) {
        String topicName = "file-topic";

        //KafkaProducer 객체
        Properties props = new Properties();

        //bootstrap-server, key.serializer.class, value.serializer.class
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_IP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String filePath = "C:\\KafkaProj_01\\practice\\src\\main\\resources\\pizza_sample.txt";
        
        sendFileMessage(producer, topicName, filePath);
        
        producer.close();
    }

    private static void sendFileMessage(KafkaProducer<String, String> producer, String topicName, String filePath) {

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line ;
            final String delimiter = ",";

            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split(delimiter);
                String key = tokens[0];
                StringBuffer value = new StringBuffer();
                for (int i = 1; i < tokens.length; i++) {
                    if (i != (tokens.length - 1)) {
                        value.append(tokens[i]).append(",");
                    } else {
                        value.append(tokens[i]);
                    }
                }

                sendMessage(producer, topicName, key, value.toString());
            }
        } catch (IOException e) {

        }

    }

    private static void sendMessage(KafkaProducer<String, String> producer, String topicName, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
        logger.info("key: {}, value: {}", key, value);

        producer.send(record, (metadata, exception) -> {
            // send가 성공했을 경우, 호출한다.
            if (exception == null) {
                System.out.println("Record sent successfully");
                logger.info(
                        "\n ###### record metadata received ###### \n" +
                                "partition: " + metadata.partition() + "\n" +
                                "offset: " + metadata.offset() + "\n" +
                                "timestamp: " + metadata.timestamp()
                );
            } else {
                logger.error("exception error from broker {}", exception.getMessage());
            }
        });
    }
}

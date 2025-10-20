package com.example.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProducerAsync {

    public static final String SERVER_IP = "192.168.56.101:9092";

    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerAsync.class.getName());

    public static void main(String[] args) throws InterruptedException {

        String topicName = "simple-topic";

        //KafkaProducer 객체
        Properties props = new Properties();

        //bootstrap-server, key.serializer.class, value.serializer.class
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_IP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //KafkaProducer Object creation
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //ProducerRecord Object creation
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "id-001", "hello worl333");

        // KafkaProducer message send
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

        Thread.sleep(3000);

    }
}

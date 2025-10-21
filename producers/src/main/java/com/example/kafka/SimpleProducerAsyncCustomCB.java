package com.example.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProducerAsyncCustomCB {

    public static final String SERVER_IP = "192.168.56.101:9092";

    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerAsyncCustomCB.class.getName());

    public static void main(String[] args) throws InterruptedException {

        String topicName = "multipart-topic";

        //KafkaProducer 객체
        Properties props = new Properties();

        //bootstrap-server, key.serializer.class, value.serializer.class
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_IP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //KafkaProducer Object creation
        try (KafkaProducer<Integer, String> producer = new KafkaProducer<>(props)) {

            //ProducerRecord Object creation
            for (int seq = 0; seq < 20; seq++) {

                ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName, seq, "hello world " + seq);
                logger.info("seq: {}", seq);
                // KafkaProducer message send

                producer.send(record, new CustomCallback(seq));
            }

            Thread.sleep(3000);
        }

    }
}

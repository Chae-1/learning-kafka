package com.example.kafka;

import com.github.javafaker.Faker;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PizzaProducerCustomPartitioner {

    public static final String SERVER_IP = "192.168.56.101:9092";

    public static final Logger logger = LoggerFactory.getLogger(PizzaProducerCustomPartitioner.class.getName());

    public static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer,
                                        String topicName, int iterCount,
                                        int interIntervalMils, int intervalMils,
                                        int intervalCount, boolean sync) {
        PizzaMessage pizzaMessage = new PizzaMessage();
        int iterSeq = 0;
        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = new Faker(random);

        while (iterSeq != iterCount) {
            HashMap<String, String> pMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, pMessage.get("key"), pMessage.get("value"));
            sendMessage(kafkaProducer, producerRecord, pMessage, sync);
            if ((intervalCount > 0) && (iterSeq % intervalCount == 0)) {
                try {
                    logger.info("###### IntervalCount:{} intervalMils: {} #####", intervalCount, intervalMils);
                    Thread.sleep(intervalMils);
                } catch (InterruptedException e) {
                    logger.error("{}", e.getMessage());
                }
            }

            if(interIntervalMils > 0) {
                try {
                    logger.info("###### interIntervalMils: {} #####", interIntervalMils);
                    Thread.sleep(interIntervalMils);
                } catch (InterruptedException e) {
                    logger.error("{}", e.getMessage());
                }
            }
        }

    }

    public static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                   ProducerRecord<String, String> producerRecord,
                                   HashMap<String, String> pMessage, boolean sync) {
        if (!sync) {
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                // send가 성공했을 경우, 호출한다.
                if (exception == null) {
                    System.out.println("Record sent successfully");
                    logger.info("async message: {}, partition: {}, offset={}", pMessage.get("value"), metadata.partition(), metadata.offset());
                } else {
                    logger.error("exception error from broker {}", exception.getMessage());
                }
            });
        } else {
            try {
                RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
                logger.info(
                        "\n ###### record metadata received ###### \n" +
                                "partition: " + recordMetadata.partition() +
                                "offset: " + recordMetadata.offset() +
                                "timestamp: " + recordMetadata.timestamp()
                );

            } catch (InterruptedException | ExecutionException e) {
                logger.error(e.getMessage());
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {

        String topicName = "pizza-topic";

        //KafkaProducer 객체
        Properties props = new Properties();

        //bootstrap-server, key.serializer.class, value.serializer.class
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_IP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.put(ProducerConfig.ACKS_CONFIG, "0");
//        props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
//        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "32000");
        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.example.kafka.CustomPartitioner");
        props.setProperty("custom.specialKey", "P001");

        //KafkaProducer Object creation
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //ProducerRecord Object creation
        sendPizzaMessage(producer, topicName, -1, 10, 100, 100, true);

        Thread.sleep(3000);

    }
}

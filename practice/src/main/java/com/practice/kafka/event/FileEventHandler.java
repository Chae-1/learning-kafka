package com.practice.kafka.event;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileEventHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(FileEventHandler.class);
    private final KafkaProducer<String, String> kafkaProducer;
    private final String topicName;
    private final boolean sync;

    public FileEventHandler(KafkaProducer<String, String> kafkaProducer, String topicName, boolean sync) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
        this.sync = sync;
    }

    @Override
    public void onMessage(MessageEvent messageEvent) throws InterruptedException, ExecutionException {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, messageEvent.key, messageEvent.value);

        if (this.sync) {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logResult(recordMetadata);
        } else {
            kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
                if (exception != null) {
                    logResult(recordMetadata);
                } else {
                    log.info("", exception);
                }
            });
        }
    }

    private static void logResult(RecordMetadata recordMetadata) {
        log.info("##### record metadata received ##### ");
        log.info("##### partition: {}", recordMetadata.partition());
        log.info("##### offset: {}", recordMetadata.offset());
        log.info("##### timestamp: {} ##### ", recordMetadata.timestamp());
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        String topicName = "file-topic";
        boolean sync = true;
        new FileEventHandler(kafkaProducer, topicName, sync);
    }
}

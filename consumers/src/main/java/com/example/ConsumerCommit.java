package com.example;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerCommit {
    public static final Logger logger = LoggerFactory.getLogger(
            ConsumerCommit.class
    );

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.57:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group_03");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "60000");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(List.of(topicName));

        // main thread
        Thread mainThread = Thread.currentThread();

        // main thread 종료 시, 별도의 thread로 kafkaConsumer wakeup 메서드 호출.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("main program starts to exit by calling wakeup");
            //kafkaConsumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

		pollCommitAsync(kafkaConsumer);
    }

	private static void pollCommitAsync(KafkaConsumer<String, String> kafkaConsumer) {
		try {
			while (true) {
				ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
				logger.info(" ############ consumerRecords count: {}", consumerRecords.count());
				for (ConsumerRecord<String, String> record : consumerRecords) {
					logger.info("record key: {},  partition: {}, record offset: {}, record value: {},", record.key(),
						record.partition(), record.offset(), record.value());
				}

				kafkaConsumer.commitAsync(new OffsetCommitCallback() {
					@Override
					public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
						if (e != null) {
							logger.error("offsets {} is not completed, error", offsets);
						} else {
							logger.info("Commit succeeded for offsets {}", offsets);
						}
					}
				});
			}
		} catch (WakeupException e) {
			logger.error(e.getMessage());
		} finally {
			kafkaConsumer.commitSync();
			logger.info("finally consumer is closing");
			kafkaConsumer.close();
		}
	}

	private static void pollCommitSync(KafkaConsumer<String, String> kafkaConsumer) {
		try {
			while (true) {
				ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
				logger.info(" ############ consumerRecords count: {}", consumerRecords.count());
				for (ConsumerRecord<String, String> record : consumerRecords) {
					logger.info("record key: {},  partition: {}, record offset: {}, record value: {},", record.key(),
						record.partition(), record.offset(), record.value());
				}

				try {
					if (!consumerRecords.isEmpty()) {
						kafkaConsumer.commitSync();
						logger.info("commit sync has been called");
					}
				} catch (CommitFailedException e) {
					logger.error(e.getMessage());
				}
			}
		} catch (WakeupException e) {
			logger.error(e.getMessage());
		} finally {
			logger.info("finally consumer is closing");
			kafkaConsumer.close();
		}
	}

	private static void pollAutoCommit(KafkaConsumer<String, String> kafkaConsumer) {
		try {
			while (true) {
				ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
				logger.info(" ############ consumerRecords count: {}", consumerRecords.count());
				for (ConsumerRecord<String, String> record : consumerRecords) {
					logger.info("record key: {},  partition: {}, record offset: {}, record value: {},", record.key(),
						record.partition(), record.offset(), record.value());
				}

				try {
					logger.info("main thread is sleeping {} ms during while loop", 10000);
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} catch (WakeupException e) {
			logger.error(e.getMessage());
		} finally {
			logger.info("finally consumer is closing");
			kafkaConsumer.close();
		}
	}
}

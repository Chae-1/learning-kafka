package com.practice.kafka.producer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class FileProducer {

	public static final String SERVER_IP = "192.168.56.101:9092";

	public static void main(String[] args) {

		String topicName = "file-topic";

		//KafkaProducer 객체
		Properties props = new Properties();

		//bootstrap-server, key.serializer.class, value.serializer.class
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_IP);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		//KafkaProducer Object creation
		KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		String filePath = "C:\\Users\\enssel\\Desktop\\files\\study\\learning-kafka\\practice\\src\\main\\resources\\pizza_sample.txt";
		// kafkaProducer 생성 -> ProducerRecords 생성 -> send 비동기 방식 전송
		sendFileMessage(producer, topicName, filePath);

	}

	private static void sendFileMessage(KafkaProducer<String, String> producer, String topicName, String filePath) {
		try (
			BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
		) {
			String line;
			while ((line = bufferedReader.readLine()) != null) {

			}

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}

package com.github.timtebeek;

import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class TestConsumer<K, V> implements Closeable {

	private KafkaConsumer<K, V> consumer;

	public TestConsumer(String topic) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		try {
			consumer = new KafkaConsumer<>(props);
		} catch (Exception e) {
			log.error("exception creating Consumer", e);
			throw e;
		}
		consumer.subscribe(Arrays.asList(topic));
	}

	public void blockUntilReady() {
		consumer.listTopics(Duration.ofSeconds(5));
	}

	public ConsumerRecords<K, V> doConsume() {
		log.debug("polling for records");
		return consumer.poll(Duration.ofSeconds(1));
	}

	@Override
	public void close() {
		consumer.close();
	}
}

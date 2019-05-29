package com.github.timtebeek;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.Future;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class TestProducer<K, V> implements Closeable {

	private Producer<K, V> producer;

	public TestProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "0");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("buffer.memory", 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

		try {
			producer = new KafkaProducer<>(props);
		} catch (Exception e) {
			log.error("exception creating Producer", e);
			throw e;
		}
	}

	public Future<RecordMetadata> doProduce(ProducerRecord<K, V> record) {
		log.debug("sending message");
		return producer.send(record, (recordMetadata, e) -> {
			if (e != null) {
				log.error("exception sending message", e);
			}
		});
	}

	@Override
	public void close() {
		producer.close();
		log.debug("producer closed");
	}
}

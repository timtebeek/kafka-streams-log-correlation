package com.github.timtebeek;

import java.util.Map;

import brave.kafka.clients.KafkaTracing;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

@SpringBootApplication
@EnableKafkaStreams
@Slf4j
public class KafkaStreamsLogCorrelationApplication {

	public static void main(String... args) {
		SpringApplication.run(KafkaStreamsLogCorrelationApplication.class, args);
	}

	@Bean
	public KStream<String, String> doStream(StreamsBuilder builder) throws Exception {
		KStream<String, String> numbersStream = builder.stream("numbers");

		KStream<String, String>[] branches = numbersStream.branch(
				(k, v) -> Integer.valueOf(v) % 2 == 0,
				(k, v) -> true);

		branches[0].peek((k, v) -> log.info("Even: {} -> {}", k, v)).to("even-numbers");
		branches[1].peek((k, v) -> log.info("Odd: {} -> {}", k, v)).to("odd-numbers");

		return numbersStream;
	}

	@Bean
	public KafkaClientSupplier kafkaClientSupplier(StreamsBuilderFactoryBean defaultKafkaStreamsBuilder, KafkaTracing kafkaTracing) {
		MyTracingKafkaClientSupplier clientSupplier = new MyTracingKafkaClientSupplier(kafkaTracing);
		// Rather than setting ClientSupplier like this, we try creating pointcuts as in SleuthKafkaAspect
		defaultKafkaStreamsBuilder.setClientSupplier(clientSupplier);
		return clientSupplier;
	}
}

@RequiredArgsConstructor
class MyTracingKafkaClientSupplier extends DefaultKafkaClientSupplier {

	private final KafkaTracing kafkaTracing;

	@Override
	public Producer<byte[], byte[]> getProducer(Map<String, Object> config) {
		return kafkaTracing.producer(super.getProducer(config));
	}

	@Override
	public Consumer<byte[], byte[]> getConsumer(Map<String, Object> config) {
		return kafkaTracing.consumer(super.getConsumer(config));
	}

	@Override
	public Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> config) {
		return kafkaTracing.consumer(super.getRestoreConsumer(config));
	}

	@Override
	public Consumer<byte[], byte[]> getGlobalConsumer(Map<String, Object> config) {
		return kafkaTracing.consumer(super.getGlobalConsumer(config));
	}
}

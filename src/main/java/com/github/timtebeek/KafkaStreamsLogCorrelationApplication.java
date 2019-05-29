package com.github.timtebeek;

import java.util.Map;

import brave.kafka.clients.KafkaTracing;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

@SpringBootApplication
// @EnableKafkaStreams // Bean override conflict when enabled
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

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME)
	public StreamsBuilderFactoryBean defaultKafkaStreamsBuilder(KafkaTracing kafkaTracing,
			@Qualifier(KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME) ObjectProvider<KafkaStreamsConfiguration> streamsConfigProvider) {
		StreamsBuilderFactoryBean streamsBuilderFactoryBean = new KafkaStreamsDefaultConfiguration()
				.defaultKafkaStreamsBuilder(streamsConfigProvider);
		streamsBuilderFactoryBean.setClientSupplier(new MyTracingKafkaClientSupplier(kafkaTracing));
		return streamsBuilderFactoryBean;
	}
}

@RequiredArgsConstructor
class MyTracingKafkaClientSupplier implements KafkaClientSupplier {

	final KafkaTracing kafkaTracing;

	@Override
	public AdminClient getAdminClient(Map<String, Object> config) {
		return AdminClient.create(config);
	}

	@Override
	public Producer<byte[], byte[]> getProducer(Map<String, Object> config) {
		return kafkaTracing.producer(new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer()));
	}

	@Override
	public Consumer<byte[], byte[]> getConsumer(Map<String, Object> config) {
		return kafkaTracing.consumer(new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer()));
	}

	@Override
	public Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> config) {
		return getConsumer(config);
	}

	@Override
	public Consumer<byte[], byte[]> getGlobalConsumer(Map<String, Object> config) {
		return getConsumer(config);
	}
}

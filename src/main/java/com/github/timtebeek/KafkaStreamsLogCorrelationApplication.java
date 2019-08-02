package com.github.timtebeek;

import brave.kafka.streams.KafkaStreamsTracing;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
@Slf4j
public class KafkaStreamsLogCorrelationApplication {

	public static void main(String... args) {
		SpringApplication.run(KafkaStreamsLogCorrelationApplication.class, args);
	}

	@Autowired
	private KafkaStreamsTracing kafkaStreamsTracing;

	@Bean
	public KStream<String, String> doStream(StreamsBuilder builder) throws Exception {
		KStream<String, String> numbersStream = builder.stream("numbers");

		KStream<String, String>[] branches = numbersStream.branch(
				(k, v) -> Integer.valueOf(v) % 2 == 0,
				(k, v) -> true);

		// branches[0].peek((k, v) -> log.info("Even: {} -> {}", k, v)).to("even-numbers");
		// branches[1].peek((k, v) -> log.info("Odd: {} -> {}", k, v)).to("odd-numbers");
		branches[0].transformValues(kafkaStreamsTracing.peek("even", (k, v) -> log.info("Even: {} -> {}", k, v))).to("even-numbers");
		branches[1].transformValues(kafkaStreamsTracing.peek("odd", (k, v) -> log.info("Odd: {} -> {}", k, v))).to("odd-numbers");

		return numbersStream;
	}

}

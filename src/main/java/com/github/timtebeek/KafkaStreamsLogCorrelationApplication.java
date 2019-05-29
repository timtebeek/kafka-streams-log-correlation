package com.github.timtebeek;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
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

}

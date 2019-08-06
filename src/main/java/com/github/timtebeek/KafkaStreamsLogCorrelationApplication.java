package com.github.timtebeek;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class KafkaStreamsLogCorrelationApplication {

	public static void main(String... args) {
		SpringApplication.run(KafkaStreamsLogCorrelationApplication.class, args);
	}

}

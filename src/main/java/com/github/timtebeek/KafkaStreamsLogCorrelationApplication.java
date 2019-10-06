package com.github.timtebeek;

import brave.handler.FinishedSpanHandler;
import brave.handler.MutableSpan;
import brave.propagation.TraceContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class KafkaStreamsLogCorrelationApplication {

	public static void main(String... args) {
		SpringApplication.run(KafkaStreamsLogCorrelationApplication.class, args);
	}

	@Bean
	public FinishedSpanHandler finishedSpanHandler() {
		// Suffix remote service name "kafka" with topic name for clearer dependency graph
		return new FinishedSpanHandler() {
			@Override
			public boolean handle(TraceContext context, MutableSpan span) {
				String topic = span.tag("kafka.topic");
				if (topic != null) {
					span.remoteServiceName(span.remoteServiceName() + '/' + topic);
				}
				return true;
			}
		};
	}
}

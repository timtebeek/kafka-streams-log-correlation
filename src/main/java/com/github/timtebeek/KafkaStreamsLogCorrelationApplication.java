package com.github.timtebeek;

import brave.handler.MutableSpan;
import brave.handler.SpanHandler;
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
	public SpanHandler finishedSpanHandler() {
		// Suffix remote service name "kafka" with topic name for clearer dependency graph
		return new SpanHandler() {
			@Override
			public boolean end(TraceContext context, MutableSpan span, Cause cause) {
				String topic = span.tag("kafka.topic");
				if (cause == Cause.FINISHED && topic != null) {
					span.remoteServiceName(span.remoteServiceName() + '/' + topic);
				}
				return true;
			}
		};
	}
}

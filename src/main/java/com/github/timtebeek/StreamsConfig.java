package com.github.timtebeek;

import brave.Tracer;
import brave.kafka.streams.KafkaStreamsTracing;
import brave.propagation.ExtraFieldPropagation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class StreamsConfig {

	private final KafkaStreamsTracing kafkaStreamsTracing;
	private final Tracer tracer;

	@Bean
	public KStream<String, String> doStream(StreamsBuilder builder) throws Exception {
		KStream<String, String> numbersStream = builder.stream("numbers");
		numbersStream.transformValues(kafkaStreamsTracing.peek("get", (k, v) -> {
			String value = ExtraFieldPropagation.get(tracer.currentSpan().context(), "messageid");
			log.info("got: {} for {} -> {}", value, k, v);
			log.info("MDC: {}", MDC.getCopyOfContextMap());
		}));

		@SuppressWarnings("unchecked")
		KStream<String, String>[] branches = numbersStream.branch(
				(k, v) -> Integer.valueOf(v) % 2 == 0,
				(k, v) -> true);

		branches[0].transformValues(kafkaStreamsTracing.peek("even", (k, v) -> log.info("Even: {} -> {}", k, v)))
				.to("even-numbers");
		branches[1].transformValues(kafkaStreamsTracing.peek("odd", (k, v) -> log.info("Odd: {} -> {}", k, v)))
				.to("odd-numbers");

		return numbersStream;
	}
}

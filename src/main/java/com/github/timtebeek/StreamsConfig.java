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
import org.springframework.kafka.support.KafkaStreamBrancher;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class StreamsConfig {

	private static final String NUMBERS_TOPIC = "numbers";
	private static final String ODD_NUMBERS_TOPIC = "odd-numbers";
	private static final String EVEN_NUMBERS_TOPIC = "even-numbers";

	private final KafkaStreamsTracing kafkaStreamsTracing;
	private final Tracer tracer;

	@Bean
	public KStream<String, Integer> doStream(StreamsBuilder builder) throws Exception {
		KStream<String, Integer> numbersStream = builder.stream(NUMBERS_TOPIC);
		numbersStream
				// Append diagnostic information
				.transformValues(kafkaStreamsTracing.peek("set", (k, v) -> {
					ExtraFieldPropagation.set(tracer.currentSpan().context(), "messageid", "messageid_" + v);
					log.info("set messageid for {} -> {}", k, v);
				}));

		return new KafkaStreamBrancher<String, Integer>()
				.branch((k, v) -> v % 2 == 0, evenStream -> {
					evenStream
							// Log with trace context
							.transformValues(kafkaStreamsTracing.peek("even", (k, v) -> {
								log.info("Even: {} -> {}", k, v);
								log.info("MDC: {}", MDC.getCopyOfContextMap());
							}))
							.to(EVEN_NUMBERS_TOPIC);

				})
				.defaultBranch(oddStream -> {
					oddStream
							// Log without trace context
							.peek((k, v) -> log.info("Odd: {} -> {}", k, v))
							.to(ODD_NUMBERS_TOPIC);
				})
				.onTopOf(numbersStream);
	}
}

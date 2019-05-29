package com.github.timtebeek;

import brave.kafka.clients.KafkaTracing;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.sleuth.autoconfig.TraceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.ProducerFactory;

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

	@Configuration
	@ConditionalOnProperty(value = "spring.sleuth.messaging.kafka.streams.enabled", matchIfMissing = true)
	@ConditionalOnClass(ProducerFactory.class)
	@AutoConfigureAfter({ TraceAutoConfiguration.class })
	protected static class SleuthKafkaStreamsConfiguration {
		@Bean
		public KafkaClientSupplier kafkaClientSupplier(StreamsBuilderFactoryBean defaultKafkaStreamsBuilder) {
			KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();
			defaultKafkaStreamsBuilder.setClientSupplier(clientSupplier);
			return clientSupplier;
		}

		@Bean
		SleuthKafkaStreamsAspect sleuthKafkaStreamsAspect(KafkaTracing kafkaTracing) {
			return new SleuthKafkaStreamsAspect(kafkaTracing);
		}

	}
}

@Aspect
@RequiredArgsConstructor
class SleuthKafkaStreamsAspect {

	private final KafkaTracing kafkaTracing;

	@Pointcut("execution(* org.apache.kafka.streams.KafkaClientSupplier.getProducer(..))")
	private void anyKafkaClientSupplierProducer() {
	} // NOSONAR

	@Pointcut("execution(* org.apache.kafka.streams.KafkaClientSupplier.get*Consumer(..))")
	private void anyKafkaClientSupplierConsumer() {
	} // NOSONAR

	@Around("anyKafkaClientSupplierProducer()")
	public Object wrapKafkaClientSupplierProducer(ProceedingJoinPoint pjp) throws Throwable {
		Producer producer = (Producer) pjp.proceed();
		return this.kafkaTracing.producer(producer);
	}

	@Around("anyKafkaClientSupplierConsumer()")
	public Object wrapKafkaClientSupplierConsumer(ProceedingJoinPoint pjp) throws Throwable {
		Consumer consumer = (Consumer) pjp.proceed();
		return this.kafkaTracing.consumer(consumer);
	}
}

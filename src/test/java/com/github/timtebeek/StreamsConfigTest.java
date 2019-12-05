package com.github.timtebeek;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestTemplate;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class StreamsConfigTest {

	private TestProducer<String, Integer> numberProducer;
	private TestListener<String, Integer> evenListener;
	private TestListener<String, Integer> oddListener;

	@BeforeEach
	public void setup() {
		Assumptions.assumeTrue(() -> {
			try {
				return HttpClient.newHttpClient().send(HttpRequest.newBuilder()
						.uri(URI.create("http://localhost:9411/zipkin/"))
						.build(), BodyHandlers.discarding())
						.statusCode() == 200;
			} catch (IOException | InterruptedException e) {
				log.warn("Expected Kafka and Zipkin to be running!", e);
				return false;
			}
		});
		numberProducer = new TestProducer<>();
		evenListener = new TestListener<>("even-numbers");
		oddListener = new TestListener<>("odd-numbers");
	}

	@Test
	public void verifyTracePropagationAndNewSpan() {
		String traceId = RandomStringUtils.randomNumeric(16);
		String spanId = RandomStringUtils.randomNumeric(15);
		String key = "trace-propagation-" + RandomStringUtils.randomNumeric(10);
		numberProducer.doProduce(record(traceId, spanId, key, 1));
		numberProducer.doProduce(record(traceId, spanId, key, 2));
		numberProducer.doProduce(record(traceId, spanId, key, 3));

		ConsumerRecord<String, Integer> record = evenListener.getOutputFor(cr -> cr.key().equals(key))
				.block(Duration.ofSeconds(3));

		assertEquals(2, record.value().intValue());
		String b3value = new String(record.headers().lastHeader("b3").value());
		assertThat(b3value, startsWith(traceId));
		assertThat(b3value, not(containsString("2" + spanId)));
	}

	@Test
	public void verifyLogCorrelation() {
		String traceId = RandomStringUtils.randomNumeric(16);
		String spanId = RandomStringUtils.randomNumeric(15);
		String key = "log-correlation-" + RandomStringUtils.randomNumeric(10);
		numberProducer.doProduce(record(traceId, spanId, key, 4));
		numberProducer.doProduce(record(traceId, spanId, key, 5));
		numberProducer.doProduce(record(traceId, spanId, key, 6));

		ConsumerRecord<String, Integer> record = oddListener.getOutputFor(cr -> cr.key().equals(key))
				.block(Duration.ofSeconds(3));

		assertEquals(5, record.value().intValue());
		String b3value = new String(record.headers().lastHeader("b3").value());
		assertThat(b3value, startsWith(traceId));

		// Retrieve logfile and look for TraceId
		String loglines = new RestTemplate().getForObject("http://localhost:8080/actuator/logfile", String.class);
		assertThat("Expected logfile to contain TraceId", loglines, containsString(traceId));
	}

	private static ProducerRecord<String, Integer> record(String traceId, String spanId, String key, int number) {
		ProducerRecord<String, Integer> record = new ProducerRecord<>("numbers", key, number);
		Headers headers = record.headers();
		headers.add(new RecordHeader("X-B3-TraceId", traceId.getBytes()));
		headers.add(new RecordHeader("X-B3-SpanId", (number + spanId).getBytes()));
		headers.add(new RecordHeader("X-B3-Sampled", "1".getBytes()));
		return record;
	}

	@Test
	public void verifyExtraFieldPropagation() {
		String traceId = RandomStringUtils.randomNumeric(16);
		String spanId = RandomStringUtils.randomNumeric(15);
		String key = "extra-propagation-" + RandomStringUtils.randomNumeric(10);
		ProducerRecord<String, Integer> producedRecord = record(traceId, spanId, key, 8);
		numberProducer.doProduce(producedRecord);

		ConsumerRecord<String, Integer> record = evenListener.getOutputFor(cr -> cr.key().equals(key))
				.block(Duration.ofSeconds(3));

		// Assert message_id added to propagated information
		assertEquals(8, record.value().intValue());
		String b3value = new String(record.headers().lastHeader("b3").value());
		assertThat(b3value, startsWith(traceId));
		assertEquals("messageid_8", new String(record.headers().lastHeader("messageid").value()));

		// Assert message_id logged within application
		String loglines = new RestTemplate().getForObject("http://localhost:8080/actuator/logfile", String.class);
		assertThat("Expected logfile to contain message_id", loglines, containsString("messageid_8"));
	}

}

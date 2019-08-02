package com.github.timtebeek;

import java.time.Duration;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.web.client.RestTemplate;

import static org.hamcrest.CoreMatchers.containsString;

@Ignore("needs local Kafka")
public class KafkaStreamsLogCorrelationApplicationTests {

	private TestProducer<String, String> numberProducer = new TestProducer<>();

	private TestListener<String, String> evenListener;
	private TestListener<String, String> oddListener;

	@Before
	public void setup() {
		numberProducer = new TestProducer<>();
		evenListener = new TestListener<>("even-numbers");
		oddListener = new TestListener<>("odd-numbers");
	}

	@Test
	public void verifyTracePropagationAndNewSpan() {
		String traceId = RandomStringUtils.randomNumeric(32);
		String spanId = RandomStringUtils.randomNumeric(15);
		String key = "trace-propagation";
		produce(traceId, spanId, key, 1);
		produce(traceId, spanId, key, 2);
		produce(traceId, spanId, key, 3);

		ConsumerRecord<String, String> record = evenListener.getOutputFor(cr -> cr.key().equals(key))
				.block(Duration.ofSeconds(3));

		Assert.assertEquals("2", record.value());
		Assert.assertEquals(traceId, new String(record.headers().lastHeader("X-B3-TraceId").value()));
		String newSpanId = new String(record.headers().lastHeader("X-B3-SpanId").value());
		Assert.assertNotEquals("Expected a new SpanId", "2" + spanId, newSpanId);
	}

	@Test
	public void verifyLogCorrelation() {
		String traceId = RandomStringUtils.randomNumeric(32);
		String spanId = RandomStringUtils.randomNumeric(15);
		String key = "log-correlation";
		produce(traceId, spanId, key, 4);
		produce(traceId, spanId, key, 5);
		produce(traceId, spanId, key, 6);

		ConsumerRecord<String, String> record = oddListener.getOutputFor(cr -> cr.key().equals(key))
				.block(Duration.ofSeconds(3));

		Assert.assertEquals("5", record.value());
		Assert.assertEquals(traceId, new String(record.headers().lastHeader("X-B3-TraceId").value()));

		// Retrieve logfile and look for TraceId
		String loglines = new RestTemplate().getForObject("http://localhost:8080/actuator/logfile", String.class);
		Assert.assertThat("Expected logfile to contain TraceId", loglines, containsString(traceId));
	}

	private void produce(String traceId, String spanId, String key, int number) {
		ProducerRecord<String, String> record = new ProducerRecord<>("numbers", key, "" + number);
		Headers headers = record.headers();
		headers.add(new RecordHeader("X-B3-TraceId", traceId.getBytes()));
		headers.add(new RecordHeader("X-B3-SpanId", (number + spanId).getBytes()));
		headers.add(new RecordHeader("X-B3-Sampled", "1".getBytes()));
		numberProducer.doProduce(record);
	}

}

package com.github.timtebeek;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.web.client.RestTemplate;

import static org.hamcrest.CoreMatchers.containsString;

@Ignore("needs local Kafka")
public class StreamsConfigTests {

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
		numberProducer.doProduce(record(traceId, spanId, key, 1));
		numberProducer.doProduce(record(traceId, spanId, key, 2));
		numberProducer.doProduce(record(traceId, spanId, key, 3));

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
		numberProducer.doProduce(record(traceId, spanId, key, 4));
		numberProducer.doProduce(record(traceId, spanId, key, 5));
		numberProducer.doProduce(record(traceId, spanId, key, 6));

		ConsumerRecord<String, String> record = oddListener.getOutputFor(cr -> cr.key().equals(key))
				.block(Duration.ofSeconds(3));

		Assert.assertEquals("5", record.value());
		Assert.assertEquals(traceId, new String(record.headers().lastHeader("X-B3-TraceId").value()));

		// Retrieve logfile and look for TraceId
		String loglines = new RestTemplate().getForObject("http://localhost:8080/actuator/logfile", String.class);
		Assert.assertThat("Expected logfile to contain TraceId", loglines, containsString(traceId));
	}

	private static ProducerRecord<String, String> record(String traceId, String spanId, String key, int number) {
		ProducerRecord<String, String> record = new ProducerRecord<>("numbers", key, "" + number);
		Headers headers = record.headers();
		headers.add(new RecordHeader("X-B3-TraceId", traceId.getBytes()));
		headers.add(new RecordHeader("X-B3-SpanId", (number + spanId).getBytes()));
		headers.add(new RecordHeader("X-B3-Sampled", "1".getBytes()));
		return record;
	}

	@Test
	public void verifyExtraFieldPropagation() {
		String traceId = RandomStringUtils.randomNumeric(32);
		String spanId = RandomStringUtils.randomNumeric(15);
		String key = "extra-propagation-" + RandomStringUtils.randomAlphanumeric(10);
		ProducerRecord<String, String> producedRecord = record(traceId, spanId, key, 7);
//		producedRecord.headers().add("messageid", "messageid_7".getBytes());
		numberProducer.doProduce(producedRecord);

		ConsumerRecord<String, String> consumedRecord = oddListener.getOutputFor(cr -> cr.key().equals(key))
				.block(Duration.ofSeconds(3));

		// Assert message_id added to propagated information
		Assert.assertEquals("7", consumedRecord.value());
		Assert.assertEquals(traceId, new String(consumedRecord.headers().lastHeader("X-B3-TraceId").value()));
		List<String> headerNames = Stream.of(consumedRecord.headers().toArray()).map(Header::key).collect(Collectors.toList());
		Assertions.assertThat(headerNames).contains("messageid");
		Assert.assertEquals("messageid_7", new String(consumedRecord.headers().lastHeader("messageid").value()));

		// Assert message_id logged within application
		String loglines = new RestTemplate().getForObject("http://localhost:8080/actuator/logfile", String.class);
		Assert.assertThat("Expected logfile to contain message_id", loglines, containsString("messageid_7"));
	}

}

package com.github.timtebeek;

import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KafkaStreamsLogCorrelationApplicationTests {

	private static final String TRACEID_VALUE = "463ac35c9f6413ad48485a3953bb6124";

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
	public void verifyEvenTracePropagation() {
		String key = "trace-propagation";
		produce(key, 1);
		produce(key, 2);
		produce(key, 3);

		ConsumerRecord<String, String> record = evenListener.getOutputFor(cr -> cr.key().equals(key))
				.block(Duration.ofSeconds(3));

		Assert.assertEquals("2", record.value());
		Assert.assertEquals(TRACEID_VALUE, new String(record.headers().lastHeader("X-B3-TraceId").value()));
		// XXX Would have somewhat expected a different spanId
		Assert.assertEquals("a2fb4a1d1a96d312", new String(record.headers().lastHeader("X-B3-SpanId").value()));
	}

	@Test
	public void verifyOddTracePropagation() {
		String key = "log-correlation";
		produce(key, 4);
		produce(key, 5);
		produce(key, 6);

		ConsumerRecord<String, String> record = oddListener.getOutputFor(cr -> cr.key().equals(key))
				.block(Duration.ofSeconds(3));

		Assert.assertEquals("5", record.value());
		Assert.assertEquals(TRACEID_VALUE, new String(record.headers().lastHeader("X-B3-TraceId").value()));
		// XXX Would have somewhat expected a different spanId
		Assert.assertEquals("a2fb4a1d1a96d315", new String(record.headers().lastHeader("X-B3-SpanId").value()));
	}

	private void produce(String key, int number) {
		ProducerRecord<String, String> record = new ProducerRecord<>("numbers", key, "" + number);
		Headers headers = record.headers();
		headers.add(new RecordHeader("X-B3-TraceId", TRACEID_VALUE.getBytes()));
		headers.add(new RecordHeader("X-B3-SpanId", ("a2fb4a1d1a96d31" + number).getBytes()));
		headers.add(new RecordHeader("X-B3-Sampled", "1".getBytes()));
		numberProducer.doProduce(record);
	}

}

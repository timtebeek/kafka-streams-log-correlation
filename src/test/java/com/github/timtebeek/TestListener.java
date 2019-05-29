package com.github.timtebeek;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@Slf4j
public class TestListener<K, V> {

	private Thread listenerThread;
	private EmitterProcessor<ConsumerRecord<K, V>> processor;
	private FluxSink<ConsumerRecord<K, V>> sink;

	public TestListener(String topic) {
		log.debug("create TestListener instance");

		processor = EmitterProcessor.create(200_000, false);
		sink = processor.sink();
		processor.subscribe();

		AtomicBoolean consumerReady = new AtomicBoolean();

		listenerThread = new Thread(() -> {
			log.debug("listener thread started");
			try (TestConsumer<K, V> testConsumer = new TestConsumer<>(topic)) {
				testConsumer.blockUntilReady();
				consumerReady.set(true);

				while (!Thread.interrupted()) {
					ConsumerRecords<K, V> records = testConsumer.doConsume();
					for (ConsumerRecord<K, V> record : records) {
						log.debug("received record");
						sink.next(record);
					}
				}
			} catch (Exception e) {
				log.warn("listener exception", e.getMessage());
			}
			log.debug("listener thread stopped");
		}, "listenerthread");
		listenerThread.setDaemon(true);
		listenerThread.start();

		while (!consumerReady.get()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				log.debug("waiting for test consumer to be ready");
			}
		}
	}

	public Mono<ConsumerRecord<K, V>> getOutputFor(Predicate<ConsumerRecord<K, V>> filterPredicate) {
		Mono<ConsumerRecord<K, V>> mono = processor
				.filter(filterPredicate)
				.next()
				.cache();
		mono.subscribe();
		return mono;
	}

	public Flux<ConsumerRecord<K, V>> getFluxFor(Predicate<ConsumerRecord<K, V>> filterPredicate) {
		return processor
				.filter(filterPredicate)
				.cache();
	}

	public void close() {
		listenerThread.interrupt();
	}
}

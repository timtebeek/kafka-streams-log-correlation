kafka-streams-log-correlation
---

Project to demonstrate Kafka Streams Log Correlation when using Sleuth for header propagation.
[Documented on my company blog](https://blog.jdriven.com/2019/10/distributed-tracing-with-kafka-streams/).

1. `docker-compose up`
2. Run `KafkaStreamsLogCorrelationApplication`
3. Run `StreamsConfigTest`
4. Open http://localhost:9411/zipkin/
5. `docker-compose rm -f && docker volume prune`

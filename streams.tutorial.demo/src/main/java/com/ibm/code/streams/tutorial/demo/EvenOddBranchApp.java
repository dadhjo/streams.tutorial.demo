package com.ibm.code.streams.tutorial.demo;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;

public class EvenOddBranchApp {
	public static final String INTEGER_TOPIC_NAME = "integer";
	public static final String EVEN_TOPIC_NAME = "even";
	public static final String ODD_TOPIC_NAME = "odd";

	/**
	 * @return
	 */
	/**
	 * @return
	 */
	public static Properties createProperties() {
//		Properties props = new Properties();
//		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "even-odd-branch");
//		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
//		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//		return props;
		
		Properties props = new Properties();
		props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,"es2-demo-inst-kafka-bootstrap-event-streams.cluster-cp4i-dallas-26b207c28b952c7c5197277ecd4a13a8-0000.us-south.containers.appdomain.cloud:443");

		// SCRAM Properties
		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
		props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
		String saslJaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required "
		        + "username=\"demo-liberty\" password=\"ov064VquwLCH\";";
		props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

		// TLS Properties
		props.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
		props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
		props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "C://tmp//es-cert.jks");
		props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "RpS9mF9igmxi");
		
		
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "even-odd-branch");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		return props;
	}

	public static Topology createTopology() {
		final StreamsBuilder builder = new StreamsBuilder();
		KStream<Integer, String> stream = builder.stream(INTEGER_TOPIC_NAME);
		
		stream.split()
	     .branch((key, value) -> key % 2 == 0, Branched.withConsumer(ks -> ks.to(EVEN_TOPIC_NAME)))
	     .branch((key, value) -> key % 2 != 0, Branched.withConsumer(ks -> ks.to(ODD_TOPIC_NAME)));
	     // .defaultBranch(Branched.withConsumer(ks->ks.to("C")));
		
		/*
		stream.filter((key, value) -> key % 2 == 0)
			  .to(EVEN_TOPIC_NAME);
		
		stream.filter((key, value) -> key % 2 != 0)
			  .to(EVEN_TOPIC_NAME);
				
		
		 * KStream<Integer, String>[] branches = stream .branch( (key, value) -> key % 2
		 * == 0, (key, value) -> true ); branches[0] .peek((key, value) ->
		 * System.out.printf("even: %s, %s%n", key, value)) .mapValues(v ->
		 * v.toUpperCase()) .to(EVEN_TOPIC_NAME); branches[1] .peek((key, value) ->
		 * System.out.printf("odd: %s, %s%n", key, value)) .mapValues(v ->
		 * v.toLowerCase()) .to(ODD_TOPIC_NAME);
		 */
		return builder.build();
	}

	public static void main(String[] args) {
		Properties props = createProperties();

		final Topology topology = createTopology();
		final KafkaStreams streams = new KafkaStreams(topology, props);
		final CountDownLatch latch = new CountDownLatch(1);

		Runtime.getRuntime().addShutdownHook(new Thread("kafka-streams-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			latch.await();
		} catch (Throwable e) {
			System.exit(1);
		}
		System.exit(0);
	}

}

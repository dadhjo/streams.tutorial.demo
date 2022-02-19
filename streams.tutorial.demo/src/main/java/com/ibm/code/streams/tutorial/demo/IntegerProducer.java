package com.ibm.code.streams.tutorial.demo;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class IntegerProducer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				"es2-demo-inst-kafka-bootstrap-event-streams.cluster-cp4i-dallas-26b207c28b952c7c5197277ecd4a13a8-0000.us-south.containers.appdomain.cloud:443");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

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

		Producer<Integer, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 10; i++) {
			producer.send(new ProducerRecord<Integer, String>(EvenOddBranchApp.INTEGER_TOPIC_NAME, Integer.valueOf(i),
					"Value - " + i));
		}
		producer.close();
	}
}

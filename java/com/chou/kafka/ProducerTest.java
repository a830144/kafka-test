package com.chou.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Hello world!
 *
 */
public class ProducerTest {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "[::1]:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("batch.size", "1");
		props.put("linger.ms", "20");
		props.put("enable.idempotence", "true");
		KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		ProducerRecord producerRecord = new ProducerRecord<String, String>("quickstart-events", null,null, "hello world again!");
		// producer.send(producerRecord);

		producer.send(producerRecord, new Callback() {
			public void onCompletion(RecordMetadata metadata, Exception e) {
				if (e != null) {
					System.out.println(e);

				}
			}
		});
		producer.close();
	}
}

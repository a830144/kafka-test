package com.chou.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition; 
import kafka.tools.ConsoleConsumer;


public class ConsumerTest {

	public static void main(String[] args) {
		//ConsoleConsumer.main(args);
		//Kafka consumer configuration settings
        String topicName = "quickstart-events";
        Properties props = new Properties();

        props.put("bootstrap.servers", "[::1]:9092");
        props.put("group.id", "n2");
        props.put("enable.auto.commit", "false");
        //props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("key.deserializer",
                        "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer
            <String, String>(props);

        //Kafka Consumer subscribes list of topics here.
        
        TopicPartition tp = new TopicPartition(topicName, 0);
        List<TopicPartition> tps = Arrays.asList(tp);
        consumer.assign(tps);
        //consumer.subscribe(Arrays.asList(topicName));
        
        
        HashSet<TopicPartition> partitions = new HashSet<TopicPartition>();
        for (TopicPartition partition : partitions) {
            long offset = consumer.position(partition);
            System.out.println(partition.partition() + ": " + offset);
        }
        consumer.seekToBeginning(partitions);
        
        

        //print the topic name
        System.out.println("Subscribed to topic " + topicName);
        int i = 0;

        while (true) {
        	
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String, String> record : records)

                // print the offset,key and value for the consumer records.
                System.out.printf("offset = %d, key = %s, value = %s\n",
                    record.offset(), record.key(), record.value());
        }
       

	}

}

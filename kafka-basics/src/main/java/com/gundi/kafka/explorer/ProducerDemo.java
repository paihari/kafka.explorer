package com.gundi.kafka.explorer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created by pai on 08.12.18.
 */
public class ProducerDemo {

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        System.out.println("Hello Worled");
        // Create Producer Propetries
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello from Java Program");

        producer.send(record);
        producer.flush();
        producer.close();
        System.out.println("Data Sent");
        // Send Data

    }

}

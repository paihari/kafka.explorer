package com.gundi.kafka.explorer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by pai on 08.12.18.
 */
public class ProducerDemoWithKeys {



    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

        String bootstrapServers = "localhost:9092";
        System.out.println("Hello Worled");
        logger.info("Hello from Logger");
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


        for(int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String value = "Hello from Java " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            ProducerRecord<String, String> record = new
                    ProducerRecord<String, String>(topic, key, value);


            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null) {
                        logger.info("Received Meta Data.  \n" +
                                "Topic: " + metadata.topic() +"\n" +
                                "Partition " + metadata.partition() + "\n" +
                                "Offset " + metadata.offset() + "\n" +
                                "Key " + record.key() + "\n" +
                                "Timestamp " + metadata.timestamp());


                    } else {
                        logger.error("Error while producing Message" , exception);
                        exception.printStackTrace();
                    }

                }
            });
        }

        producer.flush();
        producer.close();
        System.out.println("Data Sent");
        // Send Data

    }

}

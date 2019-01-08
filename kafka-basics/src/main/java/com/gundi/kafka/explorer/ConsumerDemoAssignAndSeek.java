package com.gundi.kafka.explorer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 * Created by pai on 08.12.18.
 */
public class ConsumerDemoAssignAndSeek {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class.getName());
        String bootstrapServers = "localhost:9092";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        // Ceaete Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        // Assign and Seek is used to replay and seek a message

        //Assign
        TopicPartition topicPartition = new TopicPartition(topic, 0);

        kafkaConsumer.assign(Arrays.asList(topicPartition));

        //Seek
        kafkaConsumer.seek(topicPartition, 15L);

        int noOfMesaagesToRead = 5;
        boolean keepOnReading = true;
        int noOfMessagesRead = 0;



        while (keepOnReading) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record: records) {
                noOfMessagesRead++;
                logger.info("Key: " + record.key() + ", Value:  " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset " + record.offset());
                if(noOfMessagesRead >= noOfMesaagesToRead) {
                    keepOnReading = false;
                    break;
                }

            }
        }
        logger.info("Exiting the Application");

        // Sucbsribe Cosnumer

        //poll for new Data





    }
}

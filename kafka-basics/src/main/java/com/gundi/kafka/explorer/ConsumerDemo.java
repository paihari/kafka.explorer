package com.gundi.kafka.explorer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

//Consumer API keys

        //u5SCNf18QcSAZf3OWFixuaQm4 (API key)
        //ALnJK4mSCOIh0JtRxGYa85ZyOQttqnffE9oxxrVveOrK3DTHHQ (API secret key)



//        Access token & access token secret
//        374758084-ExnESJ2peFoGKCfhTY10Lesiq0WhnEcYKSVOUUUn (Access token)

//        CgjtTIYO82gX8Vp8vTuST9Sy23NQBWrloyS20gVCBMCM5 (Access token secret)


/**
 * Created by pai on 08.12.18.
 */
public class ConsumerDemo {

    public static int count = 0;

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String bootstrapServers = "localhost:9092";
        //String groupId = "java_kafka_java_app_2";
        String groupId = "twitter_test";
        //String topic = "first_topic";
        String topic = "twitter_kafka_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        // Ceaete Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Collections.singleton(topic));
        int i = 0;
        while (true) {

            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            records.forEach(record ->  {
                count();
                logger.info("Key: " + record.key() + ", Value:  " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset " + record.offset());

            });

        }

        // Sucbsribe Cosnumer

        //poll for new Data





    }

    private static void count() {
        count ++;
        System.out.println("The Message Count " + count);
    }
}

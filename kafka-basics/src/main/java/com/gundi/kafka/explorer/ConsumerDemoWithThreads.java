package com.gundi.kafka.explorer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Created by pai on 08.12.18.
 */
public class ConsumerDemoWithThreads {

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    private void run()  {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());
        String bootstrapServers = "localhost:9092";
        String groupId = "java_kafka_java_app_3";
        String topic = "first_topic";

        CountDownLatch countDownLatch = new CountDownLatch(1);

        Runnable myConsumerRunnable = new ConsumerRunnable(topic, bootstrapServers,
                groupId, countDownLatch);



        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught Shutdown HooK");
            ((ConsumerRunnable)myConsumerRunnable).shutdown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }

        ));


        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("Application got Interrupted " + e);
        } finally {
            logger.info("Application is closing");
        }

    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch countDownLatch;
        private KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerRunnable(String topic,
                                String bootstrapServers,
                                String groupId,
                                CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            kafkaConsumer = new KafkaConsumer<String, String>(properties);
            kafkaConsumer.subscribe(Collections.singleton(topic));


        }

        @Override
        public void run() {
            Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    records.forEach(record ->  {
                        logger.info("Key: " + record.key() + ", Value:  " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset " + record.offset());

                    });

                }
            } catch (WakeupException e) {
                logger.info("Received Shutdown Signal");

            } finally {
                kafkaConsumer.close();
                countDownLatch.countDown();
            }

        }

        public void shutdown() {
            kafkaConsumer.wakeup();

        }
    }
}

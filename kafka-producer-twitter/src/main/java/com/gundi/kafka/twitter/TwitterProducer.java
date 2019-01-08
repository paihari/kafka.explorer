package com.gundi.kafka.twitter;

import com.google.common.collect.Interner;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by pai on 12.12.18.
 */
public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public TwitterProducer() {

    }

    public static void main(String[] args) {

        new TwitterProducer().run();

    }

    public void run() {

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        // Create Twitter Client
        Client client = createTwitterClient(msgQueue);
        client.connect();

        // Create Kafka Producer
       String topic = "twitter_kafka_topic_1";


        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook( new Thread( () -> {
            logger.info("Shutdown signal recieved");
            client.stop();
            kafkaProducer.close();
            logger.info("Shutdown done");

            }
        ));

        //Loop to send tweets to kafka

        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                 msg = msgQueue.poll(5, TimeUnit.SECONDS);
                System.out.println("Data " + msg);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
                kafkaProducer.flush();
                kafkaProducer.close();
            }
            if(msg != null) {
                logger.info(msg);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);
                kafkaProducer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(exception == null) {
                            logger.info("Received Meta Data.  \n" +
                                    "Topic: " + metadata.topic() +"\n" +
                                    "Partition " + metadata.partition() + "\n" +
                                    "Offset " + metadata.offset() + "\n" +
                                    "Timestamp " + metadata.timestamp());


                        } else {
                            logger.error("Error while producing Message" , exception);
                            exception.printStackTrace();
                        }
                    }
                });


            }


        }
        kafkaProducer.flush();
        kafkaProducer.close();
        logger.info("End of Application");

    }

    private KafkaProducer<String,String> createKafkaProducer() {

        String bootstrapServers = "localhost:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // High Throughput Settings
        //properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "40");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,  Integer.toString(32*1024));


        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        return  kafkaProducer;
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {


        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms

        List<String> terms = Lists.newArrayList("crypto");
        hosebirdEndpoint.trackTerms(terms);

        String consumerKey ="u5SCNf18QcSAZf3OWFixuaQm4";
        String consumerSecret = "ALnJK4mSCOIh0JtRxGYa85ZyOQttqnffE9oxxrVveOrK3DTHHQ";
        String token = "374758084-ExnESJ2peFoGKCfhTY10Lesiq0WhnEcYKSVOUUUn";
        String secret = "CgjtTIYO82gX8Vp8vTuST9Sy23NQBWrloyS20gVCBMCM5";


// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey,
                consumerSecret,
                token,
                secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));


        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
}

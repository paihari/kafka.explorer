package com.gundi.kafka.elasticsearch;

import com.google.gson.JsonParser;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by pai on 13.12.18.
 */
public class ElasticSearchConsumer {

    Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static void main(String[] args) throws IOException{
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());




        RestHighLevelClient restHighLevelClient = createClient();



        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer("twitter_kafka_topic_1");

        while (true) {


            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            Integer recordCount = records.count();
            logger.info("Received " + recordCount + " Records");
            BulkRequest bulkRequest = new BulkRequest();
            records.forEach(record ->  {
                //String twitterId = record.topic() + record.partition() + record.offset();
                String twitterId = getTwitterIdFromTweet(record.value());
                IndexRequest indexRequest = new
                        IndexRequest("twitter", "tweets", twitterId).source(
                                record.value(), XContentType.JSON);

                bulkRequest.add(indexRequest);

//                IndexResponse indexResponse = null;
//                try {
//                    indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//
//                String id = indexResponse.getId();
//
//                logger.info("Elastric Search Id"  + id);
//                logger.info("Twitter ID" + twitterId);
//
//                try {
//                    Thread.sleep(10);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }



            });

            if(recordCount > 0 ) {
                BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Commiting Offsets");
                kafkaConsumer.commitSync();
                logger.info("Offsets have been commited");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


            }

        }

        // restHighLevelClient.close();





    }

    private static JsonParser jsonParser = new JsonParser();
    private static String getTwitterIdFromTweet(String twitterValue) {
        return jsonParser.parse(twitterValue).getAsJsonObject().get("id_str").getAsString();



    }

    public static KafkaConsumer<String, String> createKafkaConsumer(String topic) {
        String bootstrapServers = "localhost:9092";
        String groupId = "twitter_kafka_elastic_3";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");


        // Ceaete Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));
        return kafkaConsumer;
    }

    public static RestHighLevelClient createClient() {

        // https://:@

        String hostName = "kafka-course-8957733720.eu-central-1.bonsaisearch.net";
        String userName = "ctvfyplxke";
        String password = "5dl8922jbb";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));

        RestClientBuilder restClientBuilder =
                RestClient.builder(new HttpHost(hostName, 443, "https")).setHttpClientConfigCallback(
                        new RestClientBuilder.HttpClientConfigCallback() {
                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                            }
                        }
                );

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        return restHighLevelClient;
    }
}

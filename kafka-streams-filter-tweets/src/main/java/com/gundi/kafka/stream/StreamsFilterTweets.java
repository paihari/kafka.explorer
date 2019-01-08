package com.gundi.kafka.stream;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

/**
 * Created by pai on 19.12.18.
 */
public class StreamsFilterTweets {

    public static void main(String[] args) {
        // Create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());


        // Create Topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_kafka_topic_1");
        KStream<String, String> filteredStream = inputTopic.filter((k, jsonTweet) -> {
            return getUserFollowersFromTweet(jsonTweet) > 1000;
        });
        filteredStream.to("important_tweets");


        // Build a topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.start();
        // Start stream application

    }


    private static JsonParser jsonParser = new JsonParser();
    private static Integer getUserFollowersFromTweet(String twitterValue) {
        try {
            return jsonParser.parse(twitterValue).
                    getAsJsonObject().
                    get("user").
                    getAsJsonObject().get("followers_count").
                    getAsInt();

        } catch(Exception e) {
            return 0;
        }
    }

}

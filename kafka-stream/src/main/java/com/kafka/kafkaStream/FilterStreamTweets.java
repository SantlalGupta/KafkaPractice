package com.kafka.kafkaStream;

import com.google.gson.JsonParser;
import com.kafka.core.propertyLoader.KafkaProperty;
import com.kafka.core.propertyLoader.PropertyLoader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

/**
 * This demonstrate to consume(read) data from kafka topic, perform filter on it and
 * then produce(write) filter data to another kafka topic.
 * <p>
 * To test this we need to start first producer program that will produce data to topic and then
 * after we will use this topic as input topic and perform filter on it.
 * <p>
 * create output topic:
 * kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic filter_tweets --create --partitions 3 --replication-factor 1
 * <p>
 * Consume filter data :
 * kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic filter_tweets --from-beginning
 */
public class FilterStreamTweets {
    /**
     * Similar to consumer group
     */
    static private String application_id_config = "demo-kafka-stream";
    static private String input_topic = "twitter_tweets";
    static private String outut_topic = "filter_tweets";
    static private Properties properties = new Properties();

    public static void main(String[] args) {
        //create properties

        setProperty();

        //create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic to read
        KStream<String, String> kStream = streamsBuilder.stream(input_topic);
        KStream<String, String> filterStream = kStream.filter(
                (key, jsonValue) -> extractUserFollowersInTweet(jsonValue) > 100
                // filter for tweets which has a user of over 100 followers
        );
        filterStream.to(outut_topic);

        //build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        //start our stream application
        kafkaStreams.start();
    }

    private static void setProperty() {
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                PropertyLoader.getMapProperties("config.properties").get(KafkaProperty.BOOTSTRAP_SERVERS));
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, application_id_config);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.StringSerde.class.getName());

    }

    private static JsonParser jsonParser = new JsonParser();

    private static Integer extractUserFollowersInTweet(String jsonTweets) {
        try {
            return jsonParser.parse(jsonTweets)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }
}

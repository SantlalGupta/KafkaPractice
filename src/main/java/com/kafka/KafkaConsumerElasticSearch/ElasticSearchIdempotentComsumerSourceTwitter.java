package com.kafka.KafkaConsumerElasticSearch;

import com.google.gson.JsonParser;
import com.kafka.PropertyLoader.KafkaProperty;
import com.kafka.PropertyLoader.PropertyLoader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * This class take data from Twitter and send data to elastic search
 * This class create idempotent consumer, it will read data, process it and then it will commit offsets to kafka.
 * So if consumer goes down than it can read message again with last committed offset,
 * As it is idempotent consumer elastic search will override previous message with this new one,
 * we don't get any issues as both messages are same.
 *
 * This class takes one record and insert into Elastic search.
 */
public class ElasticSearchIdempotentComsumerSourceTwitter {
    static Logger logger = LoggerFactory.getLogger(ElasticSearchIdempotentComsumerSourceTwitter.class.getName());

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = ElasticSearchClient.createClient();

        KafkaConsumer<String,String> kafkaConsumer = createConsumer("twitter_tweets");

        //poll data
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));//new in kafka 2.0,0
            logger.info("Reading: " + records.count() + " records" );

            for (ConsumerRecord<String, String> record : records) {

                // 2 way to create idempotent, to pass unique id to indexRequest
                //1. use generic id
              //  String id = record.topic() + "_" + record.partition()+"_" + record.offset();

                // As we are reading data from twitter, data is in json format, and each data have unique id as id_str
                // So while inserting same data twice to elastic search it won't effect because this id unique for the message

                // This id is used as elastic searhc id to create idempotent consumer
                String id = extractIdFromTweets(record.value());
                IndexRequest indexRequest = new IndexRequest("twitter_el")
                        .id(id) // to make idempotent
                        .source(record.value(), XContentType.JSON);

                IndexResponse in = client.index(indexRequest, RequestOptions.DEFAULT);
                logger.info("id : " + in.getId());

                try {
                    Thread.sleep(10); // small delay
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
            logger.info("commiting offset");
            kafkaConsumer.commitSync();
        }

        // grace fully close the client
     //  client.close();
    }

    private  static JsonParser jsonParser = new JsonParser();
    private static String extractIdFromTweets(String jsonTweets) {
        return  jsonParser.parse(jsonTweets)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    private static KafkaConsumer<String,String> createConsumer(String topic_name){

        String propertyFile = "config.properties";
        String group_id = "kafka-demo-elasticSearch";

        // create consumer configs
        Map<String, String> map = PropertyLoader.getMapProperties(propertyFile);
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, map.get(KafkaProperty.BOOTSTRAP_SERVERS));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"4"); // So we can get 4 record at a time
        // create Kafka consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        //subscribe topic
        kafkaConsumer.subscribe(Arrays.asList(topic_name));

        return kafkaConsumer;
    }
}

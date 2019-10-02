package com.kafka.KafkaConsumerElasticSearch;

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
 * This is by default at least once semenctics. i.e. it will first process data(here sending data to elastic search),
 * than only offset is committed to kafka
 */
public class ElasticSearchComsumerSourceTwitter {
    static Logger logger = LoggerFactory.getLogger(ElasticSearchComsumerSourceTwitter.class.getName());

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = ElasticSearchClient.createClient();

        KafkaConsumer<String,String> kafkaConsumer = createConsumer("twitter_tweets");

        //poll data
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));//new in kafka 2.0,0
            for (ConsumerRecord<String, String> record : records) {

                IndexRequest indexRequest = new IndexRequest("twitter").source(record.value(), XContentType.JSON);

                IndexResponse in = client.index(indexRequest, RequestOptions.DEFAULT);
                String id = in.getId();
                logger.info("id : " + id);

                try {
                    Thread.sleep(1000); // small delay
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }

        // grace fully close the client
     //   client.close();
    }

    static KafkaConsumer<String,String> createConsumer(String topic_name){

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

        // create Kafka consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        //subscribe topic
        kafkaConsumer.subscribe(Arrays.asList(topic_name));

        return kafkaConsumer;
    }
}

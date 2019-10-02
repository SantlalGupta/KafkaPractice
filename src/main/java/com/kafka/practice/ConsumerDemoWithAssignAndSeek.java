package com.kafka.practice;

import com.kafka.PropertyLoader.KafkaProperty;
import com.kafka.PropertyLoader.PropertyLoader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * This class is used to demonstrated the capability of kafka assign and seek.
 * It will demonstrate that user can read data from any specific partition say parition 0 (done using assign)
 * and able to read data from any offset say offset 15 (done using seek).
 *
 * This also demonstrate that user can read any number of record as desired.
 */
public class ConsumerDemoWithAssignAndSeek {
    static String propertyFile = "config.properties";
    static String topic_name = "first_topic";
    static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithAssignAndSeek.class.getName());

    public static void main(String[] args) {

        // create consumer configs
        Properties properties = new Properties();
        Map<String,String> map = PropertyLoader.getMapProperties(propertyFile);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, map.get(KafkaProperty.BOOTSTRAP_SERVERS));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create Kafka consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        // assign and seek mostly used to replay data or fetch a specific message
        //assign
        TopicPartition partition = new TopicPartition(topic_name,0);
        kafkaConsumer.assign(Arrays.asList(partition));

        long offsetToReadFrom = 15L;

        //seek
        kafkaConsumer.seek(partition,offsetToReadFrom);

        //let say i want to read 5 message
        int numberOfMessageToRead=5;
        int numberOfMessageSoFarRead=0;
        boolean keepOnRead=true;

        //poll data
        while (keepOnRead) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));//new in kafka 2.0,0
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessageSoFarRead += 1;
                logger.info("key : " + record.key() + "  value : " + record.value());
                logger.info("partition : " + record.partition() + "  offset : " + record.offset());

                if(numberOfMessageSoFarRead >= numberOfMessageToRead) {
                    keepOnRead = false;  // to exit while loop
                    break;  // To exit for loop
                }
            }
        }

        logger.info("To exit application");
    }
}

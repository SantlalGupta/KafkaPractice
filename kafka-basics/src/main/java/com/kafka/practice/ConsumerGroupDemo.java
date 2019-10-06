package com.kafka.practice;

import com.kafka.core.propertyLoader.KafkaProperty;
import com.kafka.core.propertyLoader.PropertyLoader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * If we have group_id and running again then next time consumer will not get any data because consumer already consumed generated data.
 * So if we want data again one way is to change the group id so that this consumer will get the data from beginning.
 *
 * Consumer rebalanceing:
 * If we start multiple instance of consumer then kafka will rebalanced the topic's partition among consumers.
 * So that each consumer will be responsible to read data from it's respective partition.
 *
 * Example:
 * Here first_topic have 3 partition.
 * 1. If we start only one instance of consumer then we can see log as, this consumer will read data from all partition of topic.
 * [main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator -
 *                  [Consumer clientId=consumer-1, groupId=my_fourth_application] Successfully joined group with generation 5
 * [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator -
 *                  [Consumer clientId=consumer-1, groupId=my_fourth_application] Setting newly assigned partitions [first_topic-0, first_topic-1, first_topic-2]
 *
 * We can see this consumer is responsible to read all three partition's data
 *
 * 2. If we are starting two instance of consumer than rebalanceing will be done and one consumer will be responsible to read two parition of topic and
 *    one consumer will be responsible to read one partition
 *
 *   consumer1 logs:
 *  [main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator -
 *              [Consumer clientId=consumer-1, groupId=my_fourth_application] Attempt to heartbeat failed since group is rebalancing
 * [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator -
 *              [Consumer clientId=consumer-1, groupId=my_fourth_application] Revoking previously assigned partitions [first_topic-0, first_topic-1, first_topic-2]
 * [main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator -
 *               [Consumer clientId=consumer-1, groupId=my_fourth_application] (Re-)joining group
 * [main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator -
 *              [Consumer clientId=consumer-1, groupId=my_fourth_application] Successfully joined group with generation 6
 * [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator -
 *              [Consumer clientId=consumer-1, groupId=my_fourth_application] Setting newly assigned partitions [first_topic-0, first_topic-1]
 *
 * consumer2 logs :
 * [main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator -
 *              [Consumer clientId=consumer-1, groupId=my_fourth_application] (Re-)joining group
 * [main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator -
 *              [Consumer clientId=consumer-1, groupId=my_fourth_application] Successfully joined group with generation 6
 * [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator -
 *              [Consumer clientId=consumer-1, groupId=my_fourth_application] Setting newly assigned partitions [first_topic-2]
 *
 * 3. When 3 consumer will started than each consumer will be responsible to read data from single partition respectively.
 * consumer1 logs :
 *  [main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator -
 *                  [Consumer clientId=consumer-1, groupId=my_fourth_application] (Re-)joining group
 * [main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator -
 *                  [Consumer clientId=consumer-1, groupId=my_fourth_application] Successfully joined group with generation 11
 * [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator -
 *                  [Consumer clientId=consumer-1, groupId=my_fourth_application] Setting newly assigned partitions [first_topic-2]
 *
 * consumer2 logs :
 *[main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator -
 *                  [Consumer clientId=consumer-1, groupId=my_fourth_application] Attempt to heartbeat failed since group is rebalancing
 * [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator -
 *                  [Consumer clientId=consumer-1, groupId=my_fourth_application] Revoking previously assigned partitions [first_topic-0, first_topic-1]
 * [main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator -
 *                  [Consumer clientId=consumer-1, groupId=my_fourth_application] (Re-)joining group
 * [main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator -
 *                  [Consumer clientId=consumer-1, groupId=my_fourth_application] Successfully joined group with generation 11
 * [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator -
 *                  [Consumer clientId=consumer-1, groupId=my_fourth_application] Setting newly assigned partitions [first_topic-0]
 *
 * consumer3 logs:
 * [main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator -
 *                  [Consumer clientId=consumer-1, groupId=my_fourth_application] (Re-)joining group
 * [main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator -
 *                  [Consumer clientId=consumer-1, groupId=my_fourth_application] Successfully joined group with generation 11
 * [main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator -
 *                  [Consumer clientId=consumer-1, groupId=my_fourth_application] Setting newly assigned partitions [first_topic-1]
 *
 * If one of consumer will stop then again load balances (number of partition) among available consumer.
 * So whenever new consumer started/stopped then kafka will rebalanced partition among available consumer.
 */
public class ConsumerGroupDemo {
    static String propertyFile = "config.properties";
    static String topic_name = "first_topic";
    static String group_id = "my_fourth_application";
    static Logger logger = LoggerFactory.getLogger(ConsumerGroupDemo.class.getName());

    public static void main(String[] args) {

        // create consumer configs
        Properties properties = new Properties();

        Map<String,String> map = PropertyLoader.getMapProperties(propertyFile);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, map.get(KafkaProperty.BOOTSTRAP_SERVERS));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create Kafka consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        //subscribe topic
        kafkaConsumer.subscribe(Arrays.asList(topic_name));

        //poll data
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));//new in kafka 2.0,0
            for (ConsumerRecord<String, String> record : records) {
                logger.info("key : " + record.key() + "  value : " + record.value());
                logger.info("partition : " + record.partition() + "  offset : " + record.offset());
            }
        }
    }
}

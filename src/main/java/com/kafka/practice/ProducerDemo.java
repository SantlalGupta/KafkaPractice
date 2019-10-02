package com.kafka.practice;

import com.kafka.PropertyLoader.KafkaProperty;
import com.kafka.PropertyLoader.PropertyLoader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

/**
 * Before executing this start kafka consumer, To get result
 * kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic --group my_third_application
 */
public class ProducerDemo {
    static String propertyFile = "config.properties";
    static String topic_name="first_topic";

    public static void main(String[] args) {

        // create producer properties
        Properties properties = new Properties();

        Map<String,String> map  = PropertyLoader.getMapProperties(propertyFile);

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, map.get(KafkaProperty.BOOTSTRAP_SERVERS));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // create Produce Record
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic_name,"hello world");

        // Send data - this is asynchronous - data never send it is in background
        kafkaProducer.send(producerRecord);

        // To send data - flush producer to send data to producer
        kafkaProducer.flush();

        // flush and close producer
        kafkaProducer.close();

    }
}

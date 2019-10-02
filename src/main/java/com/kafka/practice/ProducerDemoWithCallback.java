package com.kafka.practice;

import com.kafka.PropertyLoader.KafkaProperty;
import com.kafka.PropertyLoader.PropertyLoader;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * This is used to check where data is produced
 * i.e. in which partition data was produced, what is offset of produced data
 *
 * Before executing this start kafka consumer, To get result
 * kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic --group my_third_application
 */
public class ProducerDemoWithCallback {
    static String propertyFile = "config.properties";
    static String topic_name="first_topic";
    static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getName());

    public static void main(String[] args) {

        // create producer properties
        Properties properties = new Properties();

        Map<String,String> map = PropertyLoader.getMapProperties(propertyFile);

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, map.get(KafkaProperty.BOOTSTRAP_SERVERS));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for (int i=0;i<10;i++) {

        // create Produce Record
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic_name,"hello world" + i);

            // Send data - this is asynchronous - data never send it is in background
        kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //Executes every time when record is successfully send or an exception thrown

                    if (exception == null) {
                        logger.info("Received metadata : \n" +
                                "topic : " + metadata.topic() + "\n" +
                                "partition : " + metadata.partition() + "\n" +
                                "offset : " + metadata.offset() + "\n" +
                                "Timestamp : " + metadata.timestamp());
                    } else {
                        logger.error("Exception raised while sending data to producer : " + exception.getMessage());
                    }
                }
            });
        }

        // To send data - flush producer to send data to producer
        kafkaProducer.flush();

        // flush and close producer
        kafkaProducer.close();

    }
}

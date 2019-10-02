package com.kafka.practice;

import com.kafka.PropertyLoader.KafkaProperty;
import com.kafka.PropertyLoader.PropertyLoader;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * This is used to check where data is produced
 * i.e. in which partition data was produced, what is offset of produced data
 * <p>
 * Before executing this start kafka consumer, To get result
 * kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic --group my_third_application
 */
public class ProducerDemoWithKey {
    static String propertyFile = "config.properties";
    static String topic_name = "first_topic";
    static Logger logger = LoggerFactory.getLogger(ProducerDemoWithKey.class.getName());

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // create producer properties
        Properties properties = new Properties();

        Map<String, String> map = PropertyLoader.getMapProperties(propertyFile);

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, map.get(KafkaProperty.BOOTSTRAP_SERVERS));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            String value = "Hello World " + Integer.toString(i);
            String key = "id_" + i;
            logger.info("key : " + key);

            // create Produce Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic_name, key, value);

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
            }).get(); // block the .send() to make it synchronous - don't do this in production
            // this will make call synchronous, don't do this in production. It will hamper performance
            // this is just for testing purpose to check, every time we run the code same key always go to same partition
            // This call(.get()) to print key along with on which partition this key data goes.
            // If we omit this than entire logger key printed than metadata printed so we won't be able to identify which key goes to which partition
        }
/* This result for partition 3, result will be different for if we incr/decr number of parition
id_0 p=>1
id_1 p=>0
id_2 p=>2
id_3 p=>0
id_4 p=>2
id_5 p=>2
id_6 p=>0
id_7 p=>2
id_8 p=>1
id_9 p=>2

 */
        // To send data - flush producer to send data to producer
        kafkaProducer.flush();

        // flush and close producer
        kafkaProducer.close();

    }
}

package com.kafka.practice;

import com.kafka.core.propertyLoader.KafkaProperty;
import com.kafka.core.propertyLoader.PropertyLoader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread() {
    }

    private void run() {
        String propertyFile = "config.properties";
        String topic_name = "first_topic";
        String group_id = "my_Fifth_application";

        // latch for dealing multiple threads
        CountDownLatch countDownLatch = new CountDownLatch(1);

        logger.info("creating the consumer thread");
        ConsumerRunnable consumerRunnable =
                new ConsumerRunnable(PropertyLoader.getMapProperties(propertyFile).get(KafkaProperty.BOOTSTRAP_SERVERS),
                        topic_name, group_id, countDownLatch);

        //start the thread
        Thread mythread = new Thread(consumerRunnable);
        mythread.start();

        //add shut down hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            consumerRunnable.shutdown();
            try {
                countDownLatch.wait();
            } catch (InterruptedException e) {
                logger.error("Application existed");
            }
        }));

        try {
            countDownLatch.wait();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted : " + e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
        // to deal with concurrency, to shut down consumer correctly
        private CountDownLatch latch;
        private KafkaConsumer<String, String> kafkaConsumer;
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        ConsumerRunnable(String bootstrapServer, String topic_name, String group_id, CountDownLatch latch) {
            this.latch = latch;
            // create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            // create Kafka consumer
            kafkaConsumer = new KafkaConsumer<String, String>(properties);

            //subscribe topic
            kafkaConsumer.subscribe(Arrays.asList(topic_name));
        }

        @Override
        public void run() {
            //poll data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));//new in kafka 2.0,0
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("key : " + record.key() + "  value : " + record.value());
                        logger.info("partition : " + record.partition() + "  offset : " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal");
            } finally {
                kafkaConsumer.close();
                //tell the main thread that we are done with consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // wakeUp method is a special method to interrupt consumer.poll()
            // it will throw an exception WakeupException
            kafkaConsumer.wakeup();
        }
    }
}

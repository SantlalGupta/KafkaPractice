package com.kafka.twitterProducer;

import com.google.common.collect.Lists;
import com.kafka.core.propertyLoader.KafkaProperty;
import com.kafka.core.propertyLoader.PropertyLoader;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * This class demonstrate kafka and twitter integration.
 * After reading data from kafka and we can put into the kafka producer.
 * Before running this application need to provide twitter credential in twitter.properties file.
 * While producing data make sure that topic is created, if not create kafka topic as :
 * kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic twitter_tweets --partitions 6 --replication-factor 1
 *
 * Start kafka consumer :
 * kafka-console-consumer.sh  --bootstrap-server 127.0.0.1:9092 --topic twitter_tweets
 *
 * this also demonstrate that our producer are safe using idempotence to keep record in order
 */
public class MyTwitterProducerWithSafeProducer {
    private Logger logger = LoggerFactory.getLogger(MyTwitterProducerWithSafeProducer.class.getName());

    public static void main(String[] args) {
        new MyTwitterProducerWithSafeProducer().run();
    }

    public void run() {
        logger.info("Starting Setup....");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        // create twitter client
        Client client = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();

        logger.info("Connection established with Twitter...");

        // create kafka producer
        KafkaProducer kafkaProducer = createKafkaProducer();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application....");

            logger.info("Shutting down client from twitter....");
            client.stop();
            logger.info("closing producer");
            kafkaProducer.close();
            logger.info("Done...");
        }));

        //loop and send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5L, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("Exception raised while polling message from queue :  " + e);
                client.stop();
                kafkaProducer.close();
            }
            if (msg != null) {
                logger.info("tweets : " + msg);

                kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if(e != null){
                            kafkaProducer.close();
                            logger.error("Exception raised while producing message : " + e);
                        }
                    }
                });
            }
        }
    }

    private KafkaProducer<String,String> createKafkaProducer() {
        String propertyFile = "config.properties";
        // create producer properties
        Properties properties = new Properties();
        Map<String,String> map = PropertyLoader.getMapProperties(propertyFile);

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, map.get(KafkaProperty.BOOTSTRAP_SERVERS));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        //create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");

        //optional
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5"); // for kafka 2.0 > 1.1 we can keep this as 5, use 1 otherwise

        // create producer
        return new KafkaProducer<String, String>(properties);

    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        String propertyFile = "twitter.properties";

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // list of word for which we want to get twittes
        List<String> terms = Lists.newArrayList("kafka");
        hosebirdEndpoint.trackTerms(terms);

        Map<String,String> map = PropertyLoader.getMapProperties(propertyFile);
        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(
                map.get(KafkaProperty.CONSUMER_KEY),
                map.get(KafkaProperty.CONSUMER_SECRET),
                map.get(KafkaProperty.TOKEN),
                map.get(KafkaProperty.SECRET));

        ClientBuilder builder = new ClientBuilder()
                .name("kafka twitter test")     // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}

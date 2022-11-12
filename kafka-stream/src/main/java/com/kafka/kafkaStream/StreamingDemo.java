
package com.kafka.kafkaStream;

import com.kafka.XMLParser.JavaJdbc;
import com.kafka.XMLParser.Student;
import com.kafka.XMLParser.XMLParsing;
import com.kafka.core.propertyLoader.KafkaProperty;
import com.kafka.core.propertyLoader.PropertyLoader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;


import java.util.List;
import java.util.Properties;


/**
 * This class demonstrate reading from Kafka topic and inserting record to MySql database
 * <p>
 * To test this we need to start first producer program that will produce data to topic and
 * then open consumer who will consume it
 * <p>
 * kafka-console-producer.sh --bootstrap-server localhost:9092 --topic xml_topic
 *input:
 *    <?xml version = "1.0"?> <class>    <student rollno = "393">       <firstname>dinkar</firstname>       <lastname>kad</lastname>       <nickname>dinkar</nickname>       <marks>85</marks>    </student> </class>
 * <p>
 * Consume  data :
 * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic second_topic --from-beginning
 */


public class StreamingDemo {
    /**
     * Similar to consumer group
     */
    static private String application_id_config = "demo-kafka-stream";
    static private String input_topic = "xml_topic";
    static private String outut_topic = "second_topic";
    static private Properties properties = new Properties();

    public static void main(String[] args) {
        //create properties

        setProperty();

        //create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic to read
        KStream<String, String> kStream = streamsBuilder.stream(input_topic);

        //Print key and value on console
        kStream.foreach((key, value) ->
        {
            System.out.println(value);
            if(!value.isEmpty() && value!=null) {
                List<Student> stuList = XMLParsing.getStudentList(value);
                JavaJdbc.insertRecordIntoMysql(stuList);
            }
        });

        //send data on output topic
        KStream<String, String> filterStream = kStream.filter(
                (key, jsonValue) -> !jsonValue.isEmpty()
                //extractUserFollowersInTweet(jsonValue) > 100
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

}

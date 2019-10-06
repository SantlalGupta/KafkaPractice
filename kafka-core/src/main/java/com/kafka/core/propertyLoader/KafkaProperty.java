package com.kafka.core.propertyLoader;

/**
 * This class is used to declare set of Kafka property used in a project.
 * This also helps to get Kafka and Twitter property from .properties file.
 */
public class KafkaProperty {

    /** Used to get kafka Bootstrap server. */
    public static final String BOOTSTRAP_SERVERS = "bootstrap_servers";

    /** Twitter consumer key. */
    public static final String CONSUMER_KEY = "consumerKey";

    /** Twitter consumer secret.*/
    public static final String CONSUMER_SECRET = "consumerSecret";

    /** Twitter user token. */
    public static final String TOKEN = "token";

    /** Twitter user Secret. */
    public static final String SECRET = "secret";

    /** Used to get type of compression used. */
    public static final String COMPRESSION_TYPE = "compression_type";

    /** Used to get wait time till kafka producer wait than send batch in one request. */
    public static final String LINGER_MS = "linger_ms";

    /** Used to get Batch size in KB. */
    public static final String BATCH_SIZE = "batch_size";

    /**Used to get elastic search username.*/
    public static final String ELASTICSEARCH_USERNAME="username";

    /**Used to get elastic search user password.*/
    public static  final String ELASTICSEARCH_PASSWORD="password";

    /**Used to get elastic search host name.*/
    public static final String ELASTICSEARCH_HOSTNAME="hostname";
}

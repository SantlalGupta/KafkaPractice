package com.kafka.KafkaConsumerElasticSearch;

import com.kafka.PropertyLoader.KafkaProperty;
import com.kafka.PropertyLoader.PropertyLoader;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This class is used to provide RestHighLevelClient.
 * It will read property from elasticSearch.properties and create RestHighLevelClient
 */
public class ElasticSearchClient {
    static String propertyFile = "elasticSearch.properties";
    static Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class.getName());

    /**
     * It will read properties from property file and do authentication based on credential provided by user.
     *
     * @return RestHighLevelClient
     */
    public static RestHighLevelClient createClient() {

        logger.info("creating RestHighLevelClient");
        Map<String, String> map = PropertyLoader.getMapProperties(propertyFile);

        // no need to use if a local Elastic search
        // As we are testing with Secure cloud mode bonsai
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(map.get(KafkaProperty.ELASTICSEARCH_USERNAME),
                        map.get(KafkaProperty.ELASTICSEARCH_PASSWORD)));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(map.get(KafkaProperty.ELASTICSEARCH_HOSTNAME), 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
}

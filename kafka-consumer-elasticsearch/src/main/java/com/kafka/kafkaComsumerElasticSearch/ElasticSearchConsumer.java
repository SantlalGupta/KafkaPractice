package com.kafka.kafkaComsumerElasticSearch;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class is used to demonstrate how to put json data to elastic search.
 * We need to first create "twitter" index in bonsai
 * To get back result we can do
 * get /twitter/tweets/<id-printed on console> on bonsai console
 */
public class ElasticSearchConsumer {

    static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = ElasticSearchClient.createClient();
        String source = "{\"foo\":\"bar\"}";
        IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(source, XContentType.JSON);

        IndexResponse in = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = in.getId();
        logger.info("id : " + id);

        // grace fully close the client
        client.close();
    }
}

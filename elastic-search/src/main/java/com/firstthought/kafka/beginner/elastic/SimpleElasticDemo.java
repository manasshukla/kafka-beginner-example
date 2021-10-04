package com.firstthought.kafka.beginner.elastic;

import com.firstthought.kafka.beginner.example.props.ConsumerProperties;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;

/**
 * This class is just a rough and dirty implementation of how to publish record in an index
 */
public class SimpleElasticDemo {
    private static final KafkaConsumer<String, String> simpleConsumer = new KafkaConsumer<>(ConsumerProperties.getInstance().getProperties());
    private static final RestHighLevelClient elasticClient = new RestHighLevelClient(
            RestClient.builder(new HttpHost("localhost", 9200, "http")));
    private static final Logger logger = LoggerFactory.getLogger(SimpleElasticDemo.class);
    private static final String index = "twitter";
    private static final String topic = "twitter-topic";


    public static void main(String[] args) throws IOException {
        consumeFromKafka(topic);
    }

    private static String consumeFromKafka(String topic) {
        simpleConsumer.subscribe(Collections.singleton(topic));
        while (true) {
            ConsumerRecords<String, String> records = simpleConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                if(record.value()!=null) {//This is to safeguard against cases where there is no new tweet with the search terms
                    pushToElastic(index, record.value());
                }
            }
        }
    }

    private static void pushToElastic(String index, String document) {
        IndexRequest indexRequest = new IndexRequest(index)
                .source(document, XContentType.JSON);
        try {
            IndexResponse response = elasticClient.index(indexRequest, RequestOptions.DEFAULT);
            logger.info("Push to elastic successful. Id : {}, Type: {}", response.getId(), response.getType());
        } catch (IOException e) {
            logger.error("Push to elastic failed with exception ", e);
        }
    }


}

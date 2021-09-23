package com.firstthought.kafka.beginner.example.producers;

import com.firstthought.kafka.beginner.example.props.ProducerProperties;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * In this example, we use the twitter stream APIs to get new tweets with a search term,
 * and then use a Kafka producer to push the tweets to a Kafka topic
 * We have used the hosebird twitter API to interact with twitter
 * To make the APIs work one has to create a developer account with Twitter and get the tokens and API keys from there
 * Set the following environment variables :
 * 1. twitter.consumer.key
 * 2. twitter.consumer.secret
 * 3. twitter.token
 * 4. twitter.token.secret
 */
public class TwitterProducer {

    public static final String TWITTER_TOPIC = "twitter-topic";
    private static KafkaProducer<String, String> producer = new KafkaProducer<>(ProducerProperties.getInstance().getProperties());
    private static final String consumerKey = System.getenv("twitter.consumer.key");
    private static final String consumerSecret = System.getenv("twitter.consumer.secret");
    private static final String token = System.getenv("twitter.token");
    private static final String tokenSecret = System.getenv("twitter.token.secret");

    public static void main(String[] args) throws InterruptedException {
        if (checkCredentials()) {
            fetchTweets("manas", "shukla", "kafka");
        } else {
            throw new RuntimeException("Credentials not set. " +
                    "Set the following environment variables :\n" +
                    "  1. twitter.consumer.key\n" +
                    "  2. twitter.consumer.secret\n" +
                    "  3. twitter.token\n" +
                    "  4. twitter.token.secret");
        }
    }

    /**
     *
     * @return false if environmnet variables are not set
     */
    private static boolean checkCredentials() {
        return (consumerKey != null || consumerSecret != null || token != null || tokenSecret != null);
    }

    /**
     * Search twitter stream for any on the search terms mentioned
     * @param searchTerms A list of strings to search for
     * @throws InterruptedException
     */
    private static void fetchTweets(String... searchTerms) throws InterruptedException {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(10);
        Client hoseBirdClient = createHoseBirdClient(msgQueue, Arrays.asList(searchTerms));
        hoseBirdClient.connect();

        while (!hoseBirdClient.isDone()) {
            publishMessage(msgQueue.poll(1, TimeUnit.SECONDS));
        }
    }

    /**
     * @param message String to be published to the topic
     */
    private static void publishMessage(String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TWITTER_TOPIC, message);
        producer.send(record);
    }

    private static BasicClient createHoseBirdClient(BlockingQueue<String> msgQueue, List<String> searchTerms) {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(searchTerms);
        Authentication authentication = new OAuth1(consumerKey, consumerSecret,
                token, tokenSecret);
        ClientBuilder builder = new ClientBuilder().authentication(authentication)
                .name("sample-twitter-client")
                .endpoint(hosebirdEndpoint).hosts(hosebirdHosts)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}

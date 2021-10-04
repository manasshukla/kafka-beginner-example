package com.firstthought.kafka.beginner.example.consumer;

import com.firstthought.kafka.beginner.example.props.ConsumerProperties;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * There are three steps to create a Kafka Producer :
 * 1. Create Consumer Properties
 * <p>
 * 2. Create the Consumer
 * <p>
 * 3. Make the consumer subscribe to a topic
 * <p>
 * 4. Read the ConsumerRecords for the topic.
 * <b>This has to be done in the while(true) loop other wise the consumer group does not get created.</b>
 */
public class SimpleConsumerDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumerDemo.class);

    public static void main(String[] args) throws IOException {
        String topic = "twitter-topic";
        int timeout = 100;
        Properties consumerProps = ConsumerProperties.getInstance().getProperties();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);

        consumer.subscribe(Collections.singleton(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(timeout));

            records.forEach(record -> LOGGER.info("Key : {} , Value : {}, Partition : {}, Offset : {}"
                    , record.key(), record.value(), record.partition(), record.offset()));
        }
    }


}

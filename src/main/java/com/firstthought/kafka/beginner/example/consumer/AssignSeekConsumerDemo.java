package com.firstthought.kafka.beginner.example.consumer;

import com.firstthought.kafka.beginner.example.props.ConsumerProperties;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Assign and Seek is using to start reading data at particular partition and offset
 * example start reading data from Partition 1 and offset 20.
 * For this we will have to remove the group id of the consumer.
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
public class AssignSeekConsumerDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(AssignSeekConsumerDemo.class);

    public static void main(String[] args) throws IOException {
        String topic = "first-topic";
        int timeout = 100;
        int partition = 1;
        long offsetToStartReading = 20L;
        int noOfMessagesToRead = 10;
        Properties consumerProps = ConsumerProperties.getInstance().getProperties();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProps);

        TopicPartition topicPartition = new TopicPartition(topic, partition);

        consumer.assign(Collections.singleton(topicPartition));

        consumer.seek(topicPartition, offsetToStartReading);

        while (true) {


            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(timeout));
            records.forEach(record -> {
                LOGGER.info("Key : {} , Value : {}, Partition : {}, Offset : {}"
                        , record.key(), record.value(), record.partition(), record.offset());
            });


        }

    }


}

package com.firstthought.kafka.beginner.example.producers;

import com.firstthought.kafka.beginner.example.props.ProducerProperties;
import java.io.IOException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CallBackProducerDemo {
    /**
     * There are three steps to create a Kafka Producer :
     * 1. Create Producer Properties
     * <p>
     * 2. Create the Producer
     * <p>
     * 3. Prepare the message that the Producer will send to a topic
     * <p>
     * 4. Send Data or Produce message to a topic
     * <p>
     * 5. Flush and Close the producer. <b>Without this the message will not be sent to Kafka</b>
     */

    private static final Logger LOGGER = LoggerFactory.getLogger(CallBackProducerDemo.class);

    public static void main(String[] args) throws IOException {

        ProducerProperties producerProperties = ProducerProperties.getInstance();

        //Create a Kafka Producer Instance
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties.getProperties());

        //Create a Kafka Producer Record This is the message that will be send to Kafka. It should be send to a topic
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first-topic", "This is the message through CallBackProducerDemo Java Client");

        //Send the message. This is async
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    LOGGER.info("Received Metadata for topic: {}, partition: {}, offsets: {}, timestamp: {}",
                            metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                } else {
                    LOGGER.error("Error occured while sending the record", exception);
                }
            }
        });

        //producer flush and close to actually send the message to the cluster
        producer.flush();
        producer.close();

    }
}

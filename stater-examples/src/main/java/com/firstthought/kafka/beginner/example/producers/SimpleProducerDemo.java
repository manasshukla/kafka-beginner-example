package com.firstthought.kafka.beginner.example.producers;

import com.firstthought.kafka.beginner.example.props.ProducerProperties;
import java.io.IOException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducerDemo {
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

    public static void main(String[] args) throws IOException {

        ProducerProperties producerProperties = ProducerProperties.getInstance();

        //Create a Kafka Producer Instance
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties.getProperties());

        //Create a Kafka Producer Record This is the message that will be send to Kafka. It should be send to a topic
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first-topic", "This is the message through SimpleProducerDemo Java Client");

        //Send the message. This is async
        producer.send(record);

        //producer flush and close to actually send the message to the cluster
        producer.flush();
        producer.close();

    }
}

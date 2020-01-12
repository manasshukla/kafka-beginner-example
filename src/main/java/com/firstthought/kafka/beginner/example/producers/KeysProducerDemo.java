package com.firstthought.kafka.beginner.example.producers;

import com.firstthought.kafka.beginner.example.props.ProducerProperties;
import java.io.IOException;
import java.util.Random;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeysProducerDemo {
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

    private static final Logger LOGGER = LoggerFactory.getLogger(KeysProducerDemo.class);

    public static void main(String[] args) throws IOException {

        ProducerProperties producerProperties = ProducerProperties.getInstance();

        String topic = "first-topic";
        int noOfPartitions = 3;
        //Create a Kafka Producer Instance
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties.getProperties());

        //Sending multiple messages
        for (int i = 1; i <= 9; i++) {
            /*
                We are doing this to show that the same key will go to the same partitions.
                Since the no of partitions are 3 in our topic,
                we are doing modulo 3 on key id to loop on those 3 partitions
             */
            final String keyID = "id_" + i % noOfPartitions;
            String value = "Message # " + new Random().nextInt();

            //Create a Kafka Producer Record This is the message that will be send to Kafka. It should be send to a topic
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, keyID, value);

            //Send the message. This is async
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        LOGGER.info("Key id : {} goes to partitions {} ", keyID, metadata.partition());
                        LOGGER.info("Received Metadata for topic: {}, partition: {}, offsets: {}, timestamp: {}",
                                metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                    } else {
                        LOGGER.error("Error occured while sending the record", exception);
                    }
                }
            });

            //producer flush and close to actually send the message to the cluster

        }

        producer.flush();
        producer.close();


    }
}

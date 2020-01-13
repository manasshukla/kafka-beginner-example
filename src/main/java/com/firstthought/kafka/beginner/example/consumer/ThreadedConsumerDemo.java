package com.firstthought.kafka.beginner.example.consumer;

import com.firstthought.kafka.beginner.example.props.ConsumerProperties;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
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
public class ThreadedConsumerDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadedConsumerDemo.class);

    public static void main(String[] args) throws IOException {
        new ThreadedConsumerDemo().run();
    }

    public void run() throws IOException {
        Properties consumerProps = ConsumerProperties.getInstance().getProperties();

        String topic = "first-topic";

        CountDownLatch latch = new CountDownLatch(1);

        LOGGER.info("Creating the consumer thread");

        //Create a Runnable
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(topic, consumerProps, latch);

        //Create the thread and start
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Caught shutdown hook");
            myConsumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOGGER.info("Application Exited");
        }));


        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Application got interrrupted", e);
        } finally {
            LOGGER.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        int timeout = 100;
        private String topic;

        public ConsumerRunnable(String topic, Properties consumerProps, CountDownLatch latch) {
            this.latch = latch;
            this.topic = topic;
            consumer = new KafkaConsumer<String, String>(consumerProps);
        }


        @Override
        public void run() {

            try {
                while (true) {
                    consumer.subscribe(Collections.singleton(topic));
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(timeout));

                    records.forEach(record -> LOGGER.info("Key : {} , Value : {}, Partition : {}, Offset : {}"
                            , record.key(), record.value(), record.partition(), record.offset()));
                }
            } catch (WakeupException e) {
                LOGGER.info("Received shutdown signal");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }
}

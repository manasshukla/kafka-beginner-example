package com.firstthought.kafka.beginner.example.props;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class ConsumerProperties {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerProperties.class);
    private static ConsumerProperties producerProps;
    private Properties properties;
    public static final String CONSUMER_PROPERTIES = "/consumer.properties";

    public static ConsumerProperties getInstance() {
        if (producerProps == null) {
            producerProps = new ConsumerProperties();
        }
        return producerProps;
    }

    public ConsumerProperties() {
        properties = new Properties();
        try {
            properties.load(getClass().getResourceAsStream(CONSUMER_PROPERTIES));
        } catch (IOException e) {
            logger.error("Unable to load consumer properties ",e);

        }
    }

    public String getProperty(String key) {
        String prop = properties.getProperty(key);
        if (null == prop) {
            return "";
        }

        return prop;
    }

    public Properties getProperties() {
        return properties;
    }

}

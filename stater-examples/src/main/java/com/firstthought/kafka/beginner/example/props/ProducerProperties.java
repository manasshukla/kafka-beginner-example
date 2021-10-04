package com.firstthought.kafka.beginner.example.props;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class ProducerProperties {

    private static final Logger logger = LoggerFactory.getLogger(ProducerProperties.class);
    private static ProducerProperties producerProps;
    private Properties properties;
    public static final String PRODUCER_PROPERTY_FILE = "/producer.properties";

    public static ProducerProperties getInstance() {
        if (producerProps == null) {
            producerProps = new ProducerProperties();
        }
        return producerProps;
    }

    public ProducerProperties() {
        properties = new Properties();
        try {
            properties.load(getClass().getResourceAsStream(PRODUCER_PROPERTY_FILE));
        } catch (IOException e) {
            logger.error("Unable to load producer properties ",e);
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

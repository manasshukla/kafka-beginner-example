package com.firstthought.kafka.beginner.example.props;

import java.io.IOException;
import java.util.Properties;

public class ConsumerProperties {

    private static ConsumerProperties producerProps;
    private Properties properties;
    public static final String CONSUMER_PROPERTIES = "/consumer.properties";

    public static ConsumerProperties getInstance() throws IOException {
        if (producerProps == null) {
            producerProps = new ConsumerProperties();
        }
        return producerProps;
    }

    public ConsumerProperties() throws IOException {
        properties = new Properties();
        properties.load(getClass().getResourceAsStream(CONSUMER_PROPERTIES));
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

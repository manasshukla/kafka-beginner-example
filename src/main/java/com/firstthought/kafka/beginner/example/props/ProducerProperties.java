package com.firstthought.kafka.beginner.example.props;

import java.io.IOException;
import java.util.Properties;

public class ProducerProperties {

    private static ProducerProperties producerProps;
    private Properties properties;
    public static final String PRODUCER_PROPERTY_FILE = "/producer.properties";

    public static ProducerProperties getInstance() throws IOException {
        if (producerProps == null) {
            producerProps = new ProducerProperties();
        }
        return producerProps;
    }

    public ProducerProperties() throws IOException {
        properties = new Properties();
        properties.load(getClass().getResourceAsStream(PRODUCER_PROPERTY_FILE));
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

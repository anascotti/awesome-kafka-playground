package com.awesome.consumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface AwesomeConsumer {

    default public Properties loadProperties(String file) {
        Properties properties = new Properties();
        try (final InputStream stream = this.getClass().getClassLoader().getResourceAsStream(file)) {
            properties.load(stream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
    
    public void handleMessage(ConsumerRecord<?, ?> record);
    public void start();
}

package com.awesome.producer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class AwesomeProducerApplication {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        AwesomeProducerApplication app = new AwesomeProducerApplication();
        
        KafkaProducer<String, String> awesomeProducer = new KafkaProducer<>(app.loadProperties());
        
        try {
            for (int i = 0; i < 50; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("awesome_topic", "Key-00" + i, "Msg " + i);
                final Future<RecordMetadata> future = awesomeProducer.send(record);
                app.displayRecordMetaData(record, future);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            awesomeProducer.close();
        }
        
    }
    
    public Properties loadProperties() {
        Properties properties = new Properties();
        try (final InputStream stream = this.getClass().getClassLoader().getResourceAsStream("producer.properties")) {
            properties.load(stream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
    
    private void displayRecordMetaData(final ProducerRecord<String, String> record,
            final Future<RecordMetadata> future) throws InterruptedException, ExecutionException {
        final RecordMetadata recordMetadata = future.get();
        System.out.println(String.format("\nkey=%s, value=%s " + "\nsent to topic=%s part=%d off=%d at time=%s",
                record.key(), record.value(), recordMetadata.topic(), recordMetadata.partition(),
                recordMetadata.offset(), new Date(recordMetadata.timestamp())));
    }
}

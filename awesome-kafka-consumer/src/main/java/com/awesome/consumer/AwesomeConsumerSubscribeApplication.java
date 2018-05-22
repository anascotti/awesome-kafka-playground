package com.awesome.consumer;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class AwesomeConsumerSubscribeApplication implements AwesomeConsumer{

    public static void main(String[] args) {
        AwesomeConsumerSubscribeApplication app = new AwesomeConsumerSubscribeApplication();
        app.start();
    }

    @Override
    public void start() {
        KafkaConsumer<String, String> awesomeConsumer = new KafkaConsumer<>(loadProperties("consumer1.properties"));
        awesomeConsumer.subscribe(Arrays.asList("awesome_topic"));
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = awesomeConsumer.poll(10000); // msg retrievel cycle, single threaded
                System.out.println("Records received: " + records.count());
                records.forEach(this::handleMessage);
                awesomeConsumer.commitAsync((offsets, ex) -> {
                    offsets.forEach((k,v) -> System.out.println("k=" + k + "v=" + v ));
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            awesomeConsumer.close();
        }
    }

    @Override
    public void handleMessage(ConsumerRecord<?, ?> record) {
        System.out.println(String.format("\ntopic=%s, partition=%d " + "\noffset=%d key=%s value=%s",
                record.topic(), record.partition(), record.offset(), record.key(), record.value()));
    }

}

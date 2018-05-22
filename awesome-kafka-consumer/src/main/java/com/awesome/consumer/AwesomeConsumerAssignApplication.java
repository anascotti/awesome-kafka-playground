package com.awesome.consumer;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class AwesomeConsumerAssignApplication implements AwesomeConsumer{

    public static void main(String[] args) {
        AwesomeConsumerAssignApplication app = new AwesomeConsumerAssignApplication();
        app.start();
    }

    @Override
    public void handleMessage(ConsumerRecord<?, ?> record) {
        System.out.println(String.format("\ntopic=%s, partition=%d " + "\noffset=%d key=%s value=%s",
                record.topic(), record.partition(), record.offset(), record.key(), record.value()));        
    }

    @Override
    public void start() {
        KafkaConsumer<String, String> awesomeConsumer = new KafkaConsumer<>(loadProperties("consumer2.properties"));
        List<TopicPartition> partitions = new ArrayList<>();
        TopicPartition partition0 = new TopicPartition("awesome_topic", 0);
        partitions.add(partition0);
        
        awesomeConsumer.assign(partitions);
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = awesomeConsumer.poll(100);
                records.forEach(this::handleMessage);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            awesomeConsumer.close();
        }
    }
    

}

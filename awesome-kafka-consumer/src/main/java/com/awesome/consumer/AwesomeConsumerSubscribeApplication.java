package com.awesome.consumer;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class AwesomeConsumerSubscribeApplication implements AwesomeConsumer{

    public static void main(String[] args) {
        AwesomeConsumerSubscribeApplication consumer1 = new AwesomeConsumerSubscribeApplication();
        Thread t1 = new Thread(() -> consumer1.start());
        t1.start();
        
        AwesomeConsumerSubscribeApplication consumer2 = new AwesomeConsumerSubscribeApplication();
        Thread t2 = new Thread(() -> consumer2.start());
        t2.start();
        
        AwesomeConsumerSubscribeApplication consumer3 = new AwesomeConsumerSubscribeApplication();
        Thread t3 = new Thread(() -> consumer3.start());
        t3.start();
    }

    @Override
    public void start() {
        KafkaConsumer<String, String> awesomeConsumer = new KafkaConsumer<>(loadProperties("consumer1.properties"));
        awesomeConsumer.subscribe(Arrays.asList("awesome_topic"));
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = awesomeConsumer.poll(10000); // msg retrievel cycle, single threaded
                records.forEach(this::handleMessage);
//                awesomeConsumer.commitAsync((offsets, ex) -> {
//                    offsets.forEach((k,v) -> System.out.println("k=" + k + "v=" + v ));
//                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            awesomeConsumer.close();
        }
    }

    @Override
    public void handleMessage(ConsumerRecord<?, ?> record) {
        String value = (String) record.value();
        System.out.println(String.format("\n thread=%s, topic=%s, partition=%d " + "\noffset=%d key=%s value=%s",
                Thread.currentThread().getName(),
                record.topic(), 
                record.partition(), 
                record.offset(), 
                record.key(), 
                value.replace("-", "").toUpperCase()));
    }

}

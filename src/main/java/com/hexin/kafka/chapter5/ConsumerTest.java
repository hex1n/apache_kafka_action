package com.hexin.kafka.chapter5;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @Author hex1n
 * @Date 2021/8/21 12:49
 * @Description 构建 consumer
 */
public class ConsumerTest {

    public static void main(String[] args) {
        String topicName = "test-topic";
        String groupID = "test-group";
        Properties props = new Properties();
        props.put("boostrap.servers", "39.105.192.123:9092"); // 必须指定
        props.put("group.id", groupID); // 必须指定
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");// 从最早的消息开始读取
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // 必须指定
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 必须指定

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName)); // 订阅 topic
        // standalone consumer
//        TopicPartition tp1 = new TopicPartition(topicName, 1);
//        TopicPartition tp2 = new TopicPartition(topicName, 2);
//        consumer.assign(Arrays.asList(tp1, tp2));
        // 使用正则表达示方式订阅
      /*  consumer.subscribe(Pattern.compile("kafka-.*"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            }
        });*/
//        consumer.subscribe(Pattern.compile("kafka-.*"),new NoOpConsumerRebalanceListener());

        try {
            for (; ; ) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset= %d, key = %s, value=%s%n", record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }

        /*try {
            for (; ; ) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset= %d, key = %s, value=%s%n", record.offset(), record.key(), record.value());
                }
            }
        } catch (WakeupException e) {

        } finally {
            consumer.close();
        }*/
    }
}

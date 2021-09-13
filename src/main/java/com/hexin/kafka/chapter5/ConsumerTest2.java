package com.hexin.kafka.chapter5;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @Author hex1n
 * @Date 2021/8/21 20:40
 * @Description 手动提交部分分区位移
 */
public class ConsumerTest2 {

    private static volatile boolean isRunning = true;

    public static void main(String[] args) {

        String topicName = "test-topic";
        String groupID = "test-group";
        Properties props = new Properties();
        props.put("boostrap.servers", "39.105.192.123:9092"); // 必须指定
        props.put("group.id", groupID); // 必须指定
        props.put("enable.auto.commit", "false");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // 必须指定
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 必须指定

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName)); // 订阅 topic
        try {
            while (isRunning) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println(record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }
}

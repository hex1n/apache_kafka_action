package com.hexin.kafka.chapter5;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @Author hex1n
 * @Date 2021/8/21 12:49
 * @Description 构建 手动提交位移
 */
public class ConsumerManualCommitTest {

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
        final int minBatchSize = 500;
        List<ConsumerRecord> buffer = new ArrayList<>();
        for (; ; ) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                // insertIntoDb(buffer);
                consumer.commitSync();
                buffer.clear();
            }
        }
    }
}

package com.hexin.kafka.chapter5;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Author hex1n
 * @Date 2021/8/21 21:40
 * @Description 消费线程类, 执行真正的消费任务
 */
public class ConsumerRunnable implements Runnable {

    private final KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(String brokerList, String groupId, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic)); // 本例使用 分区副本自动分配策略
    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(200);
            // 本例使用 200 毫秒作为获取的超时时间
            for (ConsumerRecord<String, String> record : records) {
                // 这里面写处理消息的逻辑,本例简单的打印消息
                System.out.println(Thread.currentThread().getName()
                        + " consumed " + record.partition() + "the message with offset:" + record.offset());
            }
        }
    }
}

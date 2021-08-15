package com.hexin.kafka.chapter4;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.RetriableException;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @Author hex1n
 * @Date 2021/8/15 10:13
 * @Description 构造一个 producer
 */
public class ProducerTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "39.105.192.123:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // 必须指定
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 必须指定

        props.put("acks", "1");
        // 或者
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        props.put("retries", "3");
        props.put("batch.size", "323840");
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        // 或者
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put("max.block.ms", 3000);

        props.put("compression.type", "lz4");
        // 或者
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        props.put("retries", 100);
        // 或者
        props.put(ProducerConfig.RETRIES_CONFIG, 100);

        props.put("batch.size", 1048576);
        // 或者
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1048576);

        props.put("linger.ms", 100);
        // 或者
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);

        props.put("max.request.size", 1048576);
        // 或者
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1048576);

        props.put("request.timeout.ms", 60000);
        // 或者
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);

        props.put("partitioner.class","com.hexin.kafka.chapter4.AuditPartitioner");
        // 或者
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.hexin.kafka.chapter4.AuditPartitioner");



        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 150; i < 160; i++) {
            producer.send(new ProducerRecord<>("test-topic", Integer.toString(i), Integer.toString(i)));
        }

        // 异步发送
        for (int i = 140; i < 150; i++) {
            ProducerRecord record = new ProducerRecord<>("test-topic", Integer.toString(i), Integer.toString(i));
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        // 消息发送成功
                        System.out.println(recordMetadata.offset() + " 成功发送");
                    } else {
                        // 执行错误处理逻辑
                    }
                }
            });
        }
        // 同步发送
        for (int i = 90; i < 99; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", Integer.toString(i), Integer.toString(i));
            producer.send(record).get();
        }
        // 可重试异常与不可重试异常 区分
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "key", "value1");
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    // 消息发送成功
                    System.out.println("消息发送成功");
                } else {
                    if (e instanceof RetriableException) {
                        // 处理可重试瞬时异常
                    } else {
                        // 处理不可重试异常
                    }
                }
            }
        });
        producer.close();
    }
}

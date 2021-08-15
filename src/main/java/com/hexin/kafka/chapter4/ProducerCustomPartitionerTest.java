package com.hexin.kafka.chapter4;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @Author hex1n
 * @Date 2021/8/15 14:26
 * @Description
 */
public class ProducerCustomPartitionerTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "39.105.192.123:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 设置自定义分区策略
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.hexin.kafka.chapter4.AuditPartitioner");
        String topic = "test-topic";
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> nonKeyRecord = new ProducerRecord<>(topic, "non-key record");
        ProducerRecord<String, String> auditRecord = new ProducerRecord<>(topic, "audit", "audit record");
        ProducerRecord<String, String> nonAuditRecord = new ProducerRecord<>(topic, "other", "non-audit record");

        producer.send(nonKeyRecord).get();
        producer.send(nonAuditRecord).get();
        producer.send(auditRecord).get();
        producer.send(nonKeyRecord).get();
        producer.send(nonAuditRecord).get();

    }
}

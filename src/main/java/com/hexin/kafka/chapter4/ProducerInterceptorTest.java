package com.hexin.kafka.chapter4;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @Author hex1n
 * @Date 2021/8/15 20:22
 * @Description
 */
public class ProducerInterceptorTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "39.105.192.123:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 构建拦截链
        List<String> interceptors = new ArrayList<>();
        interceptors.add("com.hexin.kafka.chapter4.interceptor.TimeStampPrependerInterceptor");
        interceptors.add("com.hexin.kafka.chapter4.interceptor.CounterInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        String topic = "test-topic";
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "message" + i);
            producer.send(record).get();
        }
        // 一定要关闭 producer ,这样才会调用 interceptor 的 close 方法.
        producer.close();
    }
}

package com.hexin.kafka.chapter4.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Author hex1n
 * @Date 2021/8/15 15:17
 * @Description
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {

    private int errorCounter = 0;
    private int successCounter = 0;

    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {

        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            successCounter++;
        } else {
            errorCounter++;
        }
    }

    @Override
    public void close() {
        // 保存结果
        System.out.println("Successful send: " + successCounter);
        System.out.println("Failed send: " + errorCounter);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}

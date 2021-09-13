package com.hexin.kafka.chapter5;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author hex1n
 * @Date 2021/8/21 21:58
 * @Description consumer 多线程管理类,用于创建线程池以及为每个线程分配消息集合. 另外 consumer 位移提交也在该类中完成
 */
public class ConsumerThreadHandler<K, V> {

    private final KafkaConsumer<K, V> consumer;
    private ExecutorService executors;
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    public ConsumerThreadHandler(String brokerList, String groupId, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        consumer = new KafkaConsumer<K, V>(props);
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync(offsets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                offsets.clear();
            }
        });
    }

    /**
     * 消费主方法
     *
     * @param threadNumber
     */
    public void consume(int threadNumber) {
        executors = new ThreadPoolExecutor(
                threadNumber,
                threadNumber,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(1000),
                new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            while (true) {
                ConsumerRecords<K, V> records = consumer.poll(1000L);
                if (!records.isEmpty()) {
                    executors.submit(new ConsumerWorker<>(records, offsets));
                }
                commitOffsets();

            }
        } catch (WakeupException e) {

        } finally {
            commitOffsets();
            consumer.close();
        }
    }

    private void commitOffsets() {
        // 尽量降低 synchronized 块对 offsets 锁定的时间
        Map<TopicPartition, OffsetAndMetadata> unmodfiedMap;
        synchronized (offsets) {
            if (offsets.isEmpty()) {
                return;
            }
            unmodfiedMap = Collections.unmodifiableMap(new HashMap<>(offsets));
            offsets.clear();
        }
        consumer.commitSync(unmodfiedMap);
    }

    public void close() {
        consumer.wakeup();
        executors.shutdown();
    }

}

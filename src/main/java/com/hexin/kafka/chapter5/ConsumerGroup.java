package com.hexin.kafka.chapter5;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author hex1n
 * @Date 2021/8/21 21:47
 * @Description 消费线程管理类, 创建多个线程执行消费任务
 */
public class ConsumerGroup {
    private List<ConsumerRunnable> consumers;

    public ConsumerGroup(int consumerNum, String groupId, String topic, String brokerList) {
        consumers = new ArrayList<>(consumerNum);
        for (int i = 0; i < consumerNum; i++) {
            ConsumerRunnable consumerThread = new ConsumerRunnable(brokerList, groupId, topic);
            consumers.add(consumerThread);
        }
    }

    public void execute() {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (ConsumerRunnable task : consumers) {
            executorService.execute(task);
        }
    }
}

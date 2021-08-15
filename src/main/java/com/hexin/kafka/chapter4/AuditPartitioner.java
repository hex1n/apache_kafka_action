package com.hexin.kafka.chapter4;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @Author hex1n
 * @Date 2021/8/15 14:09
 * @Description 自定义分区策略
 */
public class AuditPartitioner implements Partitioner {

    private Random random;

    @Override
    public int partition(String topic, Object keyObj, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String key = (String) keyObj;
        List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(topic);
        int partitionCount = partitionInfos.size();
        int auditPartition = partitionCount - 1;
        return key == null || key.isEmpty() || !key.contains("audit") ? random.nextInt(partitionCount - 1) : auditPartition;
    }

    @Override
    public void close() {
        // 该方法实现必要资源的清理工作
    }

    @Override
    public void configure(Map<String, ?> map) {
        // 该方法实现必要资源的初始化工作
        random = new Random();
    }
}
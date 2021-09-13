package com.hexin.kafka.chapter5;

/**
 * @Author hex1n
 * @Date 2021/8/21 21:51
 * @Description 测试主方法类
 */
public class ConsumerMain {
    public static void main(String[] args) {
        String brokerList = "39.105.192.123:9092";
        String groupId = "testGroup1";
        String topic = "test-topic";
        int consumerNum = 3;
        ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum, groupId, topic, brokerList);
        consumerGroup.execute();
    }
}

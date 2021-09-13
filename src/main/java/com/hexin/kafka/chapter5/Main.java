package com.hexin.kafka.chapter5;

/**
 * @Author hex1n
 * @Date 2021/8/21 22:19
 * @Description
 */
public class Main {

    public static void main(String[] args) {
        String brokerList = "39.105.192.123:9092";
        String groupId = "test-group";
        String topic = "test-topic";
        ConsumerThreadHandler<byte[], byte[]> handler = new ConsumerThreadHandler<>(brokerList, groupId, topic);
        int cpuCount = Runtime.getRuntime().availableProcessors();
        Runnable runnable = new Runnable() {

            @Override
            public void run() {
                handler.consume(cpuCount);
            }
        };
        new Thread(runnable).start();

        try {
            // 20 秒后自动停止该测试程序
            Thread.sleep(20000L);
        } catch (InterruptedException e) {

        }
        System.out.println("Starting to close the consumer...");
        handler.close();
    }
}

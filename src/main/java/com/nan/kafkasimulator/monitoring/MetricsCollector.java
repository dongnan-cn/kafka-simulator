package com.nan.kafkasimulator.monitoring;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
// import java.util.logging.Logger;

/**
 * 监控指标收集器
 */
public class MetricsCollector {
    // private static final Logger LOGGER = Logger.getLogger(MetricsCollector.class.getName());

    // 存储生产者吞吐量数据
    private final Map<String, Double> producerThroughputMap = new ConcurrentHashMap<>();

    // 存储消费者吞吐量数据
    private final Map<String, Double> consumerThroughputMap = new ConcurrentHashMap<>();

    // 存储Topic延迟数据
    private final Map<String, Map<String, Long>> topicLatencyData = new ConcurrentHashMap<>();

    // 存储Broker指标数据
    private final Map<String, Map<String, Double>> brokerMetricsData = new ConcurrentHashMap<>();

    public MetricsCollector() {
        // 初始化延迟数据结构
        topicLatencyData.put("p50", new ConcurrentHashMap<>());
        topicLatencyData.put("p95", new ConcurrentHashMap<>());
        topicLatencyData.put("p99", new ConcurrentHashMap<>());

        // 初始化Broker指标数据结构
        brokerMetricsData.put("cpu", new ConcurrentHashMap<>());
        brokerMetricsData.put("memory", new ConcurrentHashMap<>());
        brokerMetricsData.put("disk", new ConcurrentHashMap<>());
        brokerMetricsData.put("incoming", new ConcurrentHashMap<>());
        brokerMetricsData.put("outgoing", new ConcurrentHashMap<>());
    }

    /**
     * 收集吞吐量数据
     */
    public ThroughputData collectThroughputData() {
        long timestamp = System.currentTimeMillis();

        // 聚合生产者和消费者的吞吐量数据
        Map<String, Double> topicThroughput = new HashMap<>();

        // 合并生产者吞吐量数据
        for (Map.Entry<String, Double> entry : producerThroughputMap.entrySet()) {
            String[] parts = entry.getKey().split(":");
            if (parts.length == 2) {
                String topic = parts[0];
                topicThroughput.merge(topic, entry.getValue(), Double::sum);
            }
        }

        // 合并消费者吞吐量数据
        for (Map.Entry<String, Double> entry : consumerThroughputMap.entrySet()) {
            String[] parts = entry.getKey().split(":");
            if (parts.length == 2) {
                String topic = parts[0];
                topicThroughput.merge(topic, entry.getValue(), Double::sum);
            }
        }

        return new ThroughputData(
            topicThroughput,
            new HashMap<>(producerThroughputMap),
            new HashMap<>(consumerThroughputMap),
            timestamp
        );
    }

    /**
     * 收集延迟数据
     */
    public LatencyData collectLatencyData() {
        long timestamp = System.currentTimeMillis();

        return new LatencyData(
            new HashMap<>(topicLatencyData.get("p50")),
            new HashMap<>(topicLatencyData.get("p95")),
            new HashMap<>(topicLatencyData.get("p99")),
            timestamp
        );
    }

    /**
     * 收集Topic吞吐量数据
     */
    public TopicThroughputData collectTopicThroughputData() {
        long timestamp = System.currentTimeMillis();

        // 聚合Topic吞吐量数据
        Map<String, Double> topicMessagesPerSecond = new HashMap<>();
        Map<String, Double> topicBytesPerSecond = new HashMap<>();

        // 只合并生产者的吞吐量数据
        for (Map.Entry<String, Double> entry : producerThroughputMap.entrySet()) {
            String[] parts = entry.getKey().split(":");
            if (parts.length == 2) {
                String topic = parts[0];
                topicMessagesPerSecond.merge(topic, entry.getValue(), Double::sum);
                // 字节/秒数据暂不实现
                topicBytesPerSecond.putIfAbsent(topic, 0.0);
            }
        }

        // 注释掉消费者吞吐量数据的合并
        /*
        for (Map.Entry<String, Double> entry : consumerThroughputMap.entrySet()) {
            String[] parts = entry.getKey().split(":");
            if (parts.length == 2) {
                String topic = parts[0];
                topicMessagesPerSecond.merge(topic, entry.getValue(), Double::sum);
                // 字节/秒数据暂不实现
                topicBytesPerSecond.putIfAbsent(topic, 0.0);
            }
        }
        */

        return new TopicThroughputData(
            topicMessagesPerSecond,
            topicBytesPerSecond,
            timestamp
        );
    }

    /**
     * 收集Broker指标数据
     */
    public BrokerMetricsData collectBrokerMetricsData() {
        long timestamp = System.currentTimeMillis();

        return new BrokerMetricsData(
            new HashMap<>(brokerMetricsData.get("cpu")),
            new HashMap<>(brokerMetricsData.get("memory")),
            new HashMap<>(brokerMetricsData.get("disk")),
            new HashMap<>(brokerMetricsData.get("incoming")),
            new HashMap<>(brokerMetricsData.get("outgoing")),
            timestamp
        );
    }

    /**
     * 更新生产者吞吐量数据
     */
    public void updateProducerThroughput(String topic, String producerId, double messagesPerSecond) {
        String key = topic + ":" + producerId;
        producerThroughputMap.put(key, messagesPerSecond);
    }

    /**
     * 更新消费者吞吐量数据
     */
    public void updateConsumerThroughput(String topic, String consumerGroupId, double messagesPerSecond) {
        String key = topic + ":" + consumerGroupId;
        consumerThroughputMap.put(key, messagesPerSecond);
    }

    /**
     * 更新Topic延迟数据
     */
    public void updateTopicLatency(String topic, long p50, long p95, long p99) {
        topicLatencyData.get("p50").put(topic, p50);
        topicLatencyData.get("p95").put(topic, p95);
        topicLatencyData.get("p99").put(topic, p99);
    }

    /**
     * 更新Broker指标数据
     */
    public void updateBrokerMetrics(String brokerId, double cpuUsage, double memoryUsage, 
                                   double diskUsage, double incomingByteRate, double outgoingByteRate) {
        brokerMetricsData.get("cpu").put(brokerId, cpuUsage);
        brokerMetricsData.get("memory").put(brokerId, memoryUsage);
        brokerMetricsData.get("disk").put(brokerId, diskUsage);
        brokerMetricsData.get("incoming").put(brokerId, incomingByteRate);
        brokerMetricsData.get("outgoing").put(brokerId, outgoingByteRate);
    }

    /**
     * 清除所有指标数据
     */
    public void clearAllMetrics() {
        producerThroughputMap.clear();
        consumerThroughputMap.clear();

        topicLatencyData.values().forEach(Map::clear);
        brokerMetricsData.values().forEach(Map::clear);
    }
    
    /**
     * 重置吞吐量数据，在每次收集数据后调用
     */
    public void resetThroughputData() {
        producerThroughputMap.clear();
        consumerThroughputMap.clear();
    }
}

package com.nan.kafkasimulator.monitoring;

import java.util.Map;

/**
 * 吞吐量数据模型
 */
public class ThroughputData {
    private final Map<String, Double> topicThroughput; // Topic -> 消息/秒
    private final Map<String, Double> producerThroughput; // 生产者ID -> 消息/秒
    private final Map<String, Double> consumerThroughput; // 消费者组ID -> 消息/秒
    private final long timestamp;

    public ThroughputData(Map<String, Double> topicThroughput, 
                         Map<String, Double> producerThroughput,
                         Map<String, Double> consumerThroughput,
                         long timestamp) {
        this.topicThroughput = topicThroughput;
        this.producerThroughput = producerThroughput;
        this.consumerThroughput = consumerThroughput;
        this.timestamp = timestamp;
    }

    public Map<String, Double> getTopicThroughput() {
        return topicThroughput;
    }

    public Map<String, Double> getProducerThroughput() {
        return producerThroughput;
    }

    public Map<String, Double> getConsumerThroughput() {
        return consumerThroughput;
    }

    public long getTimestamp() {
        return timestamp;
    }
}

package com.nan.kafkasimulator.monitoring;

import java.util.Map;

/**
 * Topic吞吐量数据模型
 */
public class TopicThroughputData {
    private final Map<String, Double> topicMessagesPerSecond; // Topic -> 消息/秒
    private final Map<String, Double> topicBytesPerSecond; // Topic -> 字节/秒
    private final long timestamp;

    public TopicThroughputData(Map<String, Double> topicMessagesPerSecond, 
                              Map<String, Double> topicBytesPerSecond,
                              long timestamp) {
        this.topicMessagesPerSecond = topicMessagesPerSecond;
        this.topicBytesPerSecond = topicBytesPerSecond;
        this.timestamp = timestamp;
    }

    public Map<String, Double> getTopicMessagesPerSecond() {
        return topicMessagesPerSecond;
    }

    public Map<String, Double> getTopicBytesPerSecond() {
        return topicBytesPerSecond;
    }

    public long getTimestamp() {
        return timestamp;
    }
}

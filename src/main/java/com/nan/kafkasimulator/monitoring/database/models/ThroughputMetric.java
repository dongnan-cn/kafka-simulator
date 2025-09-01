package com.nan.kafkasimulator.monitoring.database.models;

/**
 * 吞吐量指标数据库模型
 */
public class ThroughputMetric {
    private final long timestamp;
    private final String topic;
    private final String source; // 生产者或消费者标识
    private final double messagesPerSecond;
    private final double bytesPerSecond;

    public ThroughputMetric(long timestamp, String topic, String source, 
                           double messagesPerSecond, double bytesPerSecond) {
        this.timestamp = timestamp;
        this.topic = topic;
        this.source = source;
        this.messagesPerSecond = messagesPerSecond;
        this.bytesPerSecond = bytesPerSecond;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getTopic() {
        return topic;
    }

    public String getSource() {
        return source;
    }

    public double getMessagesPerSecond() {
        return messagesPerSecond;
    }

    public double getBytesPerSecond() {
        return bytesPerSecond;
    }
}

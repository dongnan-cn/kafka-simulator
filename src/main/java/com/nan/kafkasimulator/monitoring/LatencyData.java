package com.nan.kafkasimulator.monitoring;

import java.util.Map;

/**
 * 延迟数据模型
 */
public class LatencyData {
    private final Map<String, Long> topicP50Latency; // Topic -> P50延迟(ms)
    private final Map<String, Long> topicP95Latency; // Topic -> P95延迟(ms)
    private final Map<String, Long> topicP99Latency; // Topic -> P99延迟(ms)
    private final long timestamp;

    public LatencyData(Map<String, Long> topicP50Latency, 
                      Map<String, Long> topicP95Latency,
                      Map<String, Long> topicP99Latency,
                      long timestamp) {
        this.topicP50Latency = topicP50Latency;
        this.topicP95Latency = topicP95Latency;
        this.topicP99Latency = topicP99Latency;
        this.timestamp = timestamp;
    }

    public Map<String, Long> getTopicP50Latency() {
        return topicP50Latency;
    }

    public Map<String, Long> getTopicP95Latency() {
        return topicP95Latency;
    }

    public Map<String, Long> getTopicP99Latency() {
        return topicP99Latency;
    }

    public long getTimestamp() {
        return timestamp;
    }
}

package com.nan.kafkasimulator.monitoring.database.models;

/**
 * 延迟指标数据库模型
 */
public class LatencyMetric {
    private final long timestamp;
    private final String topic;
    private final long p50Latency; // 50百分位延迟(ms)
    private final long p95Latency; // 95百分位延迟(ms)
    private final long p99Latency; // 99百分位延迟(ms)
    private final long maxLatency; // 最大延迟(ms)

    public LatencyMetric(long timestamp, String topic, long p50Latency, 
                        long p95Latency, long p99Latency, long maxLatency) {
        this.timestamp = timestamp;
        this.topic = topic;
        this.p50Latency = p50Latency;
        this.p95Latency = p95Latency;
        this.p99Latency = p99Latency;
        this.maxLatency = maxLatency;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getTopic() {
        return topic;
    }

    public long getP50Latency() {
        return p50Latency;
    }

    public long getP95Latency() {
        return p95Latency;
    }

    public long getP99Latency() {
        return p99Latency;
    }

    public long getMaxLatency() {
        return maxLatency;
    }
}

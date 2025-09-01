package com.nan.kafkasimulator.monitoring;

import java.util.Map;

/**
 * Broker指标数据模型
 */
public class BrokerMetricsData {
    private final Map<String, Double> brokerCpuUsage; // BrokerID -> CPU使用率(%)
    private final Map<String, Double> brokerMemoryUsage; // BrokerID -> 内存使用率(%)
    private final Map<String, Double> brokerDiskUsage; // BrokerID -> 磁盘使用率(%)
    private final Map<String, Double> brokerIncomingByteRate; // BrokerID -> 入站字节速率(字节/秒)
    private final Map<String, Double> brokerOutgoingByteRate; // BrokerID -> 出站字节速率(字节/秒)
    private final long timestamp;

    public BrokerMetricsData(Map<String, Double> brokerCpuUsage,
                            Map<String, Double> brokerMemoryUsage,
                            Map<String, Double> brokerDiskUsage,
                            Map<String, Double> brokerIncomingByteRate,
                            Map<String, Double> brokerOutgoingByteRate,
                            long timestamp) {
        this.brokerCpuUsage = brokerCpuUsage;
        this.brokerMemoryUsage = brokerMemoryUsage;
        this.brokerDiskUsage = brokerDiskUsage;
        this.brokerIncomingByteRate = brokerIncomingByteRate;
        this.brokerOutgoingByteRate = brokerOutgoingByteRate;
        this.timestamp = timestamp;
    }

    public Map<String, Double> getBrokerCpuUsage() {
        return brokerCpuUsage;
    }

    public Map<String, Double> getBrokerMemoryUsage() {
        return brokerMemoryUsage;
    }

    public Map<String, Double> getBrokerDiskUsage() {
        return brokerDiskUsage;
    }

    public Map<String, Double> getBrokerIncomingByteRate() {
        return brokerIncomingByteRate;
    }

    public Map<String, Double> getBrokerOutgoingByteRate() {
        return brokerOutgoingByteRate;
    }

    public long getTimestamp() {
        return timestamp;
    }
}

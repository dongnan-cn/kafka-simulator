package com.nan.kafkasimulator.monitoring.database.models;

/**
 * Broker指标数据库模型
 */
public class BrokerMetric {
    private final long timestamp;
    private final String brokerId;
    private final double cpuUsage; // CPU使用率(%)
    private final double memoryUsage; // 内存使用率(%)
    private final double diskUsage; // 磁盘使用率(%)
    private final double incomingByteRate; // 入站字节速率(字节/秒)
    private final double outgoingByteRate; // 出站字节速率(字节/秒)
    private final double requestRate; // 请求速率(请求/秒)
    private final double requestLatencyAvg; // 平均请求延迟(ms)

    public BrokerMetric(long timestamp, String brokerId, double cpuUsage, double memoryUsage,
                       double diskUsage, double incomingByteRate, double outgoingByteRate,
                       double requestRate, double requestLatencyAvg) {
        this.timestamp = timestamp;
        this.brokerId = brokerId;
        this.cpuUsage = cpuUsage;
        this.memoryUsage = memoryUsage;
        this.diskUsage = diskUsage;
        this.incomingByteRate = incomingByteRate;
        this.outgoingByteRate = outgoingByteRate;
        this.requestRate = requestRate;
        this.requestLatencyAvg = requestLatencyAvg;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getBrokerId() {
        return brokerId;
    }

    public double getCpuUsage() {
        return cpuUsage;
    }

    public double getMemoryUsage() {
        return memoryUsage;
    }

    public double getDiskUsage() {
        return diskUsage;
    }

    public double getIncomingByteRate() {
        return incomingByteRate;
    }

    public double getOutgoingByteRate() {
        return outgoingByteRate;
    }

    public double getRequestRate() {
        return requestRate;
    }

    public double getRequestLatencyAvg() {
        return requestLatencyAvg;
    }
}

package com.nan.kafkasimulator.monitoring;


/**
 * 封装所有监控数据的类
 */
public class MonitoringData {
    private final ThroughputData throughputData;
    private final LatencyData latencyData;
    private final TopicThroughputData topicThroughputData;
    private final BrokerMetricsData brokerMetricsData;

    public MonitoringData(ThroughputData throughputData, LatencyData latencyData, 
                        TopicThroughputData topicThroughputData, BrokerMetricsData brokerMetricsData) {
        this.throughputData = throughputData;
        this.latencyData = latencyData;
        this.topicThroughputData = topicThroughputData;
        this.brokerMetricsData = brokerMetricsData;
    }

    public ThroughputData getThroughputData() {
        return throughputData;
    }

    public LatencyData getLatencyData() {
        return latencyData;
    }

    public TopicThroughputData getTopicThroughputData() {
        return topicThroughputData;
    }

    public BrokerMetricsData getBrokerMetricsData() {
        return brokerMetricsData;
    }
}

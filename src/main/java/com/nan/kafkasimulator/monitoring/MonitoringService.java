package com.nan.kafkasimulator.monitoring;

import com.nan.kafkasimulator.monitoring.database.MetricsDatabase;
import javafx.concurrent.ScheduledService;
import javafx.concurrent.Task;
import javafx.util.Duration;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 监控服务类，定期收集和更新监控数据
 */
public class MonitoringService extends ScheduledService<MonitoringData> {
    private static final Logger LOGGER = Logger.getLogger(MonitoringService.class.getName());

    private final MetricsCollector metricsCollector;
    private final MetricsDatabase metricsDatabase;

    public MonitoringService() {
        super();
        setPeriod(Duration.seconds(5)); // 每5秒更新一次

        try {
            this.metricsDatabase = MetricsDatabase.getInstance();
            this.metricsCollector = new MetricsCollector();
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize metrics database", e);
            throw new RuntimeException("Failed to initialize monitoring service", e);
        }
    }

    @Override
    protected Task<MonitoringData> createTask() {
        return new Task<MonitoringData>() {
            @Override
            protected MonitoringData call() throws Exception {
                try {
                    // 收集所有监控数据
                    ThroughputData throughputData = metricsCollector.collectThroughputData();
                    LatencyData latencyData = metricsCollector.collectLatencyData();
                    TopicThroughputData topicThroughputData = metricsCollector.collectTopicThroughputData();
                    BrokerMetricsData brokerMetricsData = metricsCollector.collectBrokerMetricsData();

                    // 保存到数据库
                    saveMetricsToDatabase(throughputData, latencyData, brokerMetricsData);

                    // 返回聚合的监控数据
                    return new MonitoringData(throughputData, latencyData, topicThroughputData, brokerMetricsData);
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Failed to collect monitoring data", e);
                    throw e;
                }
            }

            private void saveMetricsToDatabase(ThroughputData throughputData, LatencyData latencyData, 
                                             BrokerMetricsData brokerMetricsData) throws SQLException {
                long timestamp = System.currentTimeMillis();

                // 保存吞吐量数据
                for (Map.Entry<String, Double> entry : throughputData.getTopicThroughput().entrySet()) {
                    metricsDatabase.saveThroughputMetric(new com.nan.kafkasimulator.monitoring.database.models.ThroughputMetric(
                        timestamp,
                        entry.getKey(),
                        "topic-aggregate",
                        entry.getValue(),
                        0.0 // 字节/秒数据暂不实现
                    ));
                }

                // 保存延迟数据
                for (Map.Entry<String, Long> entry : latencyData.getTopicP50Latency().entrySet()) {
                    String topic = entry.getKey();
                    metricsDatabase.saveLatencyMetric(new com.nan.kafkasimulator.monitoring.database.models.LatencyMetric(
                        timestamp,
                        topic,
                        entry.getValue(),
                        latencyData.getTopicP95Latency().get(topic),
                        latencyData.getTopicP99Latency().get(topic),
                        0L // 最大延迟暂不实现
                    ));
                }

                // 保存Broker指标数据
                for (Map.Entry<String, Double> entry : brokerMetricsData.getBrokerCpuUsage().entrySet()) {
                    String brokerId = entry.getKey();
                    metricsDatabase.saveBrokerMetric(new com.nan.kafkasimulator.monitoring.database.models.BrokerMetric(
                        timestamp,
                        brokerId,
                        entry.getValue(),
                        brokerMetricsData.getBrokerMemoryUsage().get(brokerId),
                        brokerMetricsData.getBrokerDiskUsage().get(brokerId),
                        brokerMetricsData.getBrokerIncomingByteRate().get(brokerId),
                        brokerMetricsData.getBrokerOutgoingByteRate().get(brokerId),
                        0.0, // 请求速率暂不实现
                        0.0  // 请求延迟暂不实现
                    ));
                }
            }
        };
    }

    @Override
    protected void succeeded() {
        super.succeeded();
        LOGGER.info("Monitoring data collection completed successfully");
    }

    @Override
    protected void failed() {
        super.failed();
        LOGGER.log(Level.SEVERE, "Monitoring data collection failed", getException());
    }

    @Override
    protected void cancelled() {
        super.cancelled();
        LOGGER.info("Monitoring service cancelled");
    }
}

package com.nan.kafkasimulator.monitoring;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Broker指标收集器，定期从Kafka集群收集Broker指标数据
 */
public class BrokerMetricsCollector {
    private static final Logger LOGGER = Logger.getLogger(BrokerMetricsCollector.class.getName());

    private final AdminClient adminClient;
    private final MetricsCollector metricsCollector;
    private final ScheduledExecutorService scheduler;

    public BrokerMetricsCollector(Properties adminClientConfig) {
        this.adminClient = AdminClient.create(adminClientConfig);
        this.metricsCollector = MetricsCollectorSingleton.getInstance();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();

        // 每10秒收集一次Broker指标
        scheduler.scheduleAtFixedRate(this::collectBrokerMetrics, 0, 10, TimeUnit.SECONDS);
    }

    /**
     * 收集Broker指标数据
     */
    private void collectBrokerMetrics() {
        try {
            DescribeClusterOptions options = new DescribeClusterOptions();
            DescribeClusterResult result = adminClient.describeCluster(options);

            // 获取集群中的所有节点
            KafkaFuture<Collection<Node>> nodesFuture = result.nodes();
            Collection<Node> nodes = nodesFuture.get();

            // 为每个Broker生成模拟指标数据
            for (Node node : nodes) {
                String brokerId = String.valueOf(node.id());

                // 生成模拟指标数据
                // 在实际应用中，这些数据应该从Kafka的JMX指标或AdminClient API获取
                double cpuUsage = Math.random() * 100; // CPU使用率 (0-100%)
                double memoryUsage = Math.random() * 100; // 内存使用率 (0-100%)
                double diskUsage = Math.random() * 100; // 磁盘使用率 (0-100%)
                double incomingByteRate = Math.random() * 10000; // 入站字节率 (字节/秒)
                double outgoingByteRate = Math.random() * 10000; // 出站字节率 (字节/秒)

                // 更新指标数据
                metricsCollector.updateBrokerMetrics(
                    brokerId, 
                    cpuUsage, 
                    memoryUsage, 
                    diskUsage, 
                    incomingByteRate, 
                    outgoingByteRate
                );
            }

            LOGGER.info("Successfully collected broker metrics for " + nodes.size() + " brokers");
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.log(Level.SEVERE, "Failed to collect broker metrics", e);
        }
    }

    /**
     * 停止收集指标数据
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
        adminClient.close();
    }
}

package com.nan.kafkasimulator.monitoring.ui;

import com.nan.kafkasimulator.monitoring.BrokerMetricsData;
import com.nan.kafkasimulator.monitoring.LatencyData;
import com.nan.kafkasimulator.monitoring.MonitoringData;
import com.nan.kafkasimulator.monitoring.MonitoringService;
import com.nan.kafkasimulator.monitoring.ThroughputData;
import com.nan.kafkasimulator.monitoring.TopicThroughputData;
import javafx.application.Platform;
import javafx.concurrent.WorkerStateEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.chart.BarChart;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.control.Label;
import javafx.scene.control.TitledPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.VBox;

import java.net.URL;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 监控仪表板控制器
 */
public class MonitoringDashboardController implements Initializable {
    private static final Logger LOGGER = Logger.getLogger(MonitoringDashboardController.class.getName());

    private MonitoringService monitoringService;

    // 图表数据存储
    private final int MAX_DATA_POINTS = 20; // 最多显示20个数据点
    private final Map<String, XYChart.Series<String, Number>> throughputSeriesMap = new ConcurrentHashMap<>();
    private final Map<String, XYChart.Series<String, Number>> latencySeriesMap = new ConcurrentHashMap<>();

    @FXML private Label titleLabel;
    @FXML private TitledPane throughputPane;
    @FXML private LineChart<String, Number> systemThroughputChart;
    @FXML private BarChart<String, Number> topicThroughputChart;
    @FXML private TitledPane latencyPane;
    @FXML private LineChart<String, Number> e2eLatencyChart;
    @FXML private BarChart<String, Number> latencyDistributionChart;
    @FXML private TitledPane brokerMetricsPane;
    @FXML private GridPane brokerMetricsGrid;

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        initializeCharts();

        // 创建并启动监控服务
        monitoringService = new MonitoringService();
        monitoringService.setOnSucceeded(this::updateCharts);
        monitoringService.setOnFailed(event -> 
            LOGGER.log(Level.SEVERE, "Monitoring service failed", monitoringService.getException()));
        monitoringService.start();
    }

    private void initializeCharts() {
        // 初始化系统吞吐量图表
        systemThroughputChart.setTitle("System Throughput");
        systemThroughputChart.getXAxis().setLabel("Time");
        systemThroughputChart.getYAxis().setLabel("Messages/sec");
        systemThroughputChart.setAnimated(false);
        systemThroughputChart.setCreateSymbols(false);

        // 初始化Topic吞吐量柱状图
        topicThroughputChart.setTitle("Topic Throughput");
        topicThroughputChart.getXAxis().setLabel("Topic");
        topicThroughputChart.getYAxis().setLabel("Messages/sec");
        topicThroughputChart.setAnimated(false);

        // 初始化端到端延迟图表
        e2eLatencyChart.setTitle("End-to-End Latency");
        e2eLatencyChart.getXAxis().setLabel("Time");
        e2eLatencyChart.getYAxis().setLabel("Latency (ms)");
        e2eLatencyChart.setAnimated(false);
        e2eLatencyChart.setCreateSymbols(false);

        // 初始化延迟分布柱状图
        latencyDistributionChart.setTitle("Latency Distribution");
        latencyDistributionChart.getXAxis().setLabel("Latency Range");
        latencyDistributionChart.getYAxis().setLabel("Count");
        latencyDistributionChart.setAnimated(false);

        // 初始化Broker指标网格
        initializeBrokerMetricsGrid();
    }

    private void initializeBrokerMetricsGrid() {
        // 清空网格
        brokerMetricsGrid.getChildren().clear();

        // 添加标题行
        brokerMetricsGrid.add(new Label("Broker ID"), 0, 0);
        brokerMetricsGrid.add(new Label("CPU Usage (%)"), 1, 0);
        brokerMetricsGrid.add(new Label("Memory Usage (%)"), 2, 0);
        brokerMetricsGrid.add(new Label("Disk Usage (%)"), 3, 0);
        brokerMetricsGrid.add(new Label("Incoming (KB/s)"), 4, 0);
        brokerMetricsGrid.add(new Label("Outgoing (KB/s)"), 5, 0);
    }

    private void updateCharts(WorkerStateEvent event) {
        MonitoringData data = (MonitoringData) event.getSource().getValue();

        Platform.runLater(() -> {
            // 更新吞吐量图表
            updateThroughputCharts(data.getThroughputData());

            // 更新延迟图表
            updateLatencyCharts(data.getLatencyData());

            // 更新Topic吞吐量柱状图
            updateTopicThroughputChart(data.getTopicThroughputData());

            // 更新Broker指标网格
            updateBrokerMetricsGrid(data.getBrokerMetricsData());
        });
    }

    private void updateThroughputCharts(ThroughputData data) {
        // 获取当前时间作为X轴标签
        String timeLabel = formatTime(data.getTimestamp());

        // 更新系统总吞吐量
        double totalThroughput = data.getTopicThroughput().values().stream()
            .mapToDouble(Double::doubleValue)
            .sum();

        XYChart.Series<String, Number> totalSeries = throughputSeriesMap.computeIfAbsent(
            "Total", k -> {
                XYChart.Series<String, Number> series = new XYChart.Series<>();
                series.setName("Total");
                systemThroughputChart.getData().add(series);
                return series;
            });

        // 添加新数据点
        totalSeries.getData().add(new XYChart.Data<>(timeLabel, totalThroughput));

        // 限制数据点数量
        if (totalSeries.getData().size() > MAX_DATA_POINTS) {
            totalSeries.getData().remove(0);
        }

        // 更新X轴范围
        updateXAxisRange((CategoryAxis) systemThroughputChart.getXAxis(), totalSeries);
    }

    private void updateLatencyCharts(LatencyData data) {
        // 获取当前时间作为X轴标签
        String timeLabel = formatTime(data.getTimestamp());

        // 更新P50延迟
        for (Map.Entry<String, Long> entry : data.getTopicP50Latency().entrySet()) {
            String topic = entry.getKey();
            String seriesName = topic + " P50";

            XYChart.Series<String, Number> series = latencySeriesMap.computeIfAbsent(
                seriesName, k -> {
                    XYChart.Series<String, Number> newSeries = new XYChart.Series<>();
                    newSeries.setName(seriesName);
                    e2eLatencyChart.getData().add(newSeries);
                    return newSeries;
                });

            // 添加新数据点
            series.getData().add(new XYChart.Data<>(timeLabel, entry.getValue()));

            // 限制数据点数量
            if (series.getData().size() > MAX_DATA_POINTS) {
                series.getData().remove(0);
            }
        }

        // 更新X轴范围
        for (XYChart.Series<String, Number> series : e2eLatencyChart.getData()) {
            updateXAxisRange((CategoryAxis) e2eLatencyChart.getXAxis(), series);
        }
    }

    private void updateTopicThroughputChart(TopicThroughputData data) {
        // 清空现有数据
        topicThroughputChart.getData().clear();

        // 创建新的数据系列
        XYChart.Series<String, Number> series = new XYChart.Series<>();
        series.setName("Messages/sec");

        // 添加数据
        for (Map.Entry<String, Double> entry : data.getTopicMessagesPerSecond().entrySet()) {
            series.getData().add(new XYChart.Data<>(entry.getKey(), entry.getValue()));
        }

        topicThroughputChart.getData().add(series);
    }

    private void updateBrokerMetricsGrid(BrokerMetricsData data) {
        // 清空现有数据（保留标题行）
        brokerMetricsGrid.getChildren().clear();
        initializeBrokerMetricsGrid();

        // 添加每个Broker的数据
        int row = 1;
        for (String brokerId : data.getBrokerCpuUsage().keySet()) {
            brokerMetricsGrid.add(new Label(brokerId), 0, row);
            brokerMetricsGrid.add(new Label(String.format("%.2f", data.getBrokerCpuUsage().get(brokerId))), 1, row);
            brokerMetricsGrid.add(new Label(String.format("%.2f", data.getBrokerMemoryUsage().get(brokerId))), 2, row);
            brokerMetricsGrid.add(new Label(String.format("%.2f", data.getBrokerDiskUsage().get(brokerId))), 3, row);
            brokerMetricsGrid.add(new Label(String.format("%.2f", data.getBrokerIncomingByteRate().get(brokerId) / 1024)), 4, row);
            brokerMetricsGrid.add(new Label(String.format("%.2f", data.getBrokerOutgoingByteRate().get(brokerId) / 1024)), 5, row);
            row++;
        }
    }

    private void updateXAxisRange(CategoryAxis xAxis, XYChart.Series<String, Number> series) {
        if (!series.getData().isEmpty()) {
            // 确保X轴显示最新的数据
            xAxis.setAutoRanging(false);
            xAxis.invalidateRange(series.getData().stream()
                .map(data -> data.getXValue())
                .toList());
        }
    }

    private String formatTime(long timestamp) {
        // 简单的时间格式化，只显示时分秒
        java.time.LocalDateTime dateTime = java.time.LocalDateTime.ofInstant(
            java.time.Instant.ofEpochMilli(timestamp), 
            java.time.ZoneId.systemDefault());
        return dateTime.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
    }

    /**
     * 设置连接状态
     * @param connected 是否连接
     */
    public void setConnected(boolean connected) {
        if (connected && monitoringService != null && monitoringService.getState() == javafx.concurrent.Service.State.READY) {
            monitoringService.start();
        } else if (!connected && monitoringService != null && monitoringService.isRunning()) {
            monitoringService.cancel();
        }
    }
    
    /**
     * 清理资源
     */
    public void cleanup() {
        if (monitoringService != null && monitoringService.isRunning()) {
            monitoringService.cancel();
        }
    }
}

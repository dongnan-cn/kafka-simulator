package com.nan.kafkasimulator.monitoring.database;

import com.nan.kafkasimulator.monitoring.database.models.BrokerMetric;
import com.nan.kafkasimulator.monitoring.database.models.LatencyMetric;
import com.nan.kafkasimulator.monitoring.database.models.ThroughputMetric;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 监控指标数据库操作类
 */
public class MetricsDatabase {
    private static final Logger LOGGER = Logger.getLogger(MetricsDatabase.class.getName());
    private static final String DB_PATH = "metrics.db";
    private static MetricsDatabase instance;
    private final Connection connection;

    private MetricsDatabase() throws SQLException {
        try {
            // 加载SQLite JDBC驱动
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:" + DB_PATH);
            initializeTables();
            LOGGER.info("Metrics database initialized successfully");
        } catch (ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Failed to load SQLite JDBC driver", e);
            throw new SQLException("Failed to load SQLite JDBC driver", e);
        }
    }

    public static synchronized MetricsDatabase getInstance() throws SQLException {
        if (instance == null) {
            instance = new MetricsDatabase();
        }
        return instance;
    }

    private void initializeTables() throws SQLException {
        createThroughputTable();
        createLatencyTable();
        createBrokerMetricsTable();
    }

    private void createThroughputTable() throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS throughput_metrics (" +
                     "timestamp INTEGER NOT NULL, " +
                     "topic TEXT NOT NULL, " +
                     "source TEXT NOT NULL, " +
                     "messages_per_second REAL NOT NULL, " +
                     "bytes_per_second REAL NOT NULL" +
                     ")";

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            LOGGER.info("Throughput metrics table created or already exists");
        }

        // 创建索引
        createIndex("idx_throughput_timestamp", "throughput_metrics", "timestamp");
        createIndex("idx_throughput_topic", "throughput_metrics", "topic");
    }

    private void createLatencyTable() throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS latency_metrics (" +
                     "timestamp INTEGER NOT NULL, " +
                     "topic TEXT NOT NULL, " +
                     "p50_latency INTEGER NOT NULL, " +
                     "p95_latency INTEGER NOT NULL, " +
                     "p99_latency INTEGER NOT NULL, " +
                     "max_latency INTEGER NOT NULL" +
                     ")";

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            LOGGER.info("Latency metrics table created or already exists");
        }

        // 创建索引
        createIndex("idx_latency_timestamp", "latency_metrics", "timestamp");
        createIndex("idx_latency_topic", "latency_metrics", "topic");
    }

    private void createBrokerMetricsTable() throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS broker_metrics (" +
                     "timestamp INTEGER NOT NULL, " +
                     "broker_id TEXT NOT NULL, " +
                     "cpu_usage REAL NOT NULL, " +
                     "memory_usage REAL NOT NULL, " +
                     "disk_usage REAL NOT NULL, " +
                     "incoming_byte_rate REAL NOT NULL, " +
                     "outgoing_byte_rate REAL NOT NULL, " +
                     "request_rate REAL NOT NULL, " +
                     "request_latency_avg REAL NOT NULL" +
                     ")";

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            LOGGER.info("Broker metrics table created or already exists");
        }

        // 创建索引
        createIndex("idx_broker_timestamp", "broker_metrics", "timestamp");
        createIndex("idx_broker_id", "broker_metrics", "broker_id");
    }

    private void createIndex(String indexName, String tableName, String columnName) throws SQLException {
        String sql = String.format("CREATE INDEX IF NOT EXISTS %s ON %s (%s)", indexName, tableName, columnName);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    public void saveThroughputMetric(ThroughputMetric metric) throws SQLException {
        String sql = "INSERT INTO throughput_metrics(timestamp, topic, source, messages_per_second, bytes_per_second) " +
                     "VALUES(?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setLong(1, metric.getTimestamp());
            pstmt.setString(2, metric.getTopic());
            pstmt.setString(3, metric.getSource());
            pstmt.setDouble(4, metric.getMessagesPerSecond());
            pstmt.setDouble(5, metric.getBytesPerSecond());
            pstmt.executeUpdate();
        }
    }

    public List<ThroughputMetric> getThroughputMetrics(long startTime, long endTime) throws SQLException {
        String sql = "SELECT timestamp, topic, source, messages_per_second, bytes_per_second " +
                     "FROM throughput_metrics " +
                     "WHERE timestamp BETWEEN ? AND ? " +
                     "ORDER BY timestamp";

        List<ThroughputMetric> metrics = new ArrayList<>();
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setLong(1, startTime);
            pstmt.setLong(2, endTime);

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                metrics.add(new ThroughputMetric(
                    rs.getLong("timestamp"),
                    rs.getString("topic"),
                    rs.getString("source"),
                    rs.getDouble("messages_per_second"),
                    rs.getDouble("bytes_per_second")
                ));
            }
        }
        return metrics;
    }

    public List<ThroughputMetric> getThroughputMetricsByTopic(String topic, long startTime, long endTime) 
        throws SQLException {
        String sql = "SELECT timestamp, topic, source, messages_per_second, bytes_per_second " +
                     "FROM throughput_metrics " +
                     "WHERE topic = ? AND timestamp BETWEEN ? AND ? " +
                     "ORDER BY timestamp";

        List<ThroughputMetric> metrics = new ArrayList<>();
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, topic);
            pstmt.setLong(2, startTime);
            pstmt.setLong(3, endTime);

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                metrics.add(new ThroughputMetric(
                    rs.getLong("timestamp"),
                    rs.getString("topic"),
                    rs.getString("source"),
                    rs.getDouble("messages_per_second"),
                    rs.getDouble("bytes_per_second")
                ));
            }
        }
        return metrics;
    }

    public void saveLatencyMetric(LatencyMetric metric) throws SQLException {
        String sql = "INSERT INTO latency_metrics(timestamp, topic, p50_latency, p95_latency, p99_latency, max_latency) " +
                     "VALUES(?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setLong(1, metric.getTimestamp());
            pstmt.setString(2, metric.getTopic());
            pstmt.setLong(3, metric.getP50Latency());
            pstmt.setLong(4, metric.getP95Latency());
            pstmt.setLong(5, metric.getP99Latency());
            pstmt.setLong(6, metric.getMaxLatency());
            pstmt.executeUpdate();
        }
    }

    public List<LatencyMetric> getLatencyMetrics(long startTime, long endTime) throws SQLException {
        String sql = "SELECT timestamp, topic, p50_latency, p95_latency, p99_latency, max_latency " +
                     "FROM latency_metrics " +
                     "WHERE timestamp BETWEEN ? AND ? " +
                     "ORDER BY timestamp";

        List<LatencyMetric> metrics = new ArrayList<>();
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setLong(1, startTime);
            pstmt.setLong(2, endTime);

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                metrics.add(new LatencyMetric(
                    rs.getLong("timestamp"),
                    rs.getString("topic"),
                    rs.getLong("p50_latency"),
                    rs.getLong("p95_latency"),
                    rs.getLong("p99_latency"),
                    rs.getLong("max_latency")
                ));
            }
        }
        return metrics;
    }

    public void saveBrokerMetric(BrokerMetric metric) throws SQLException {
        String sql = "INSERT INTO broker_metrics(timestamp, broker_id, cpu_usage, memory_usage, disk_usage, " +
                     "incoming_byte_rate, outgoing_byte_rate, request_rate, request_latency_avg) " +
                     "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setLong(1, metric.getTimestamp());
            pstmt.setString(2, metric.getBrokerId());
            pstmt.setDouble(3, metric.getCpuUsage());
            pstmt.setDouble(4, metric.getMemoryUsage());
            pstmt.setDouble(5, metric.getDiskUsage());
            pstmt.setDouble(6, metric.getIncomingByteRate());
            pstmt.setDouble(7, metric.getOutgoingByteRate());
            pstmt.setDouble(8, metric.getRequestRate());
            pstmt.setDouble(9, metric.getRequestLatencyAvg());
            pstmt.executeUpdate();
        }
    }

    public List<BrokerMetric> getBrokerMetrics(long startTime, long endTime) throws SQLException {
        String sql = "SELECT timestamp, broker_id, cpu_usage, memory_usage, disk_usage, " +
                     "incoming_byte_rate, outgoing_byte_rate, request_rate, request_latency_avg " +
                     "FROM broker_metrics " +
                     "WHERE timestamp BETWEEN ? AND ? " +
                     "ORDER BY timestamp";

        List<BrokerMetric> metrics = new ArrayList<>();
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setLong(1, startTime);
            pstmt.setLong(2, endTime);

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                metrics.add(new BrokerMetric(
                    rs.getLong("timestamp"),
                    rs.getString("broker_id"),
                    rs.getDouble("cpu_usage"),
                    rs.getDouble("memory_usage"),
                    rs.getDouble("disk_usage"),
                    rs.getDouble("incoming_byte_rate"),
                    rs.getDouble("outgoing_byte_rate"),
                    rs.getDouble("request_rate"),
                    rs.getDouble("request_latency_avg")
                ));
            }
        }
        return metrics;
    }

    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}

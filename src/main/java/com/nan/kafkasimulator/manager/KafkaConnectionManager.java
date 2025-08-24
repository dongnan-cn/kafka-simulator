package com.nan.kafkasimulator.manager;

import javafx.concurrent.Task;
import javafx.scene.control.Alert;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.nan.kafkasimulator.utils.Logger.log;

/**
 * 负责管理Kafka连接的类，包括连接、断开连接和初始化生产者等操作
 */
public class KafkaConnectionManager {
    private AdminClient adminClient;
    private KafkaProducer<String, String> producer;
    private final String bootstrapServers;
    private final Consumer<Boolean> onConnectionStateChanged;

    public KafkaConnectionManager(String bootstrapServers, Consumer<Boolean> onConnectionStateChanged) {
        this.bootstrapServers = bootstrapServers;
        this.onConnectionStateChanged = onConnectionStateChanged;
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }

    public void connect() {
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            showAlert("连接错误", null, "请输入 Kafka 集群地址。");
            return;
        }

        log("正在尝试连接到 Kafka 集群: " + bootstrapServers);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "KafkaSimulator-AdminClient");

        Task<Void> connectTask = new Task<>() {
            @Override
            protected Void call() throws Exception {
                if (adminClient != null) {
                    adminClient.close();
                }
                adminClient = AdminClient.create(props);
                adminClient.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
                return null;
            }
        };

        connectTask.setOnSucceeded(event -> {
            javafx.application.Platform.runLater(() -> {
                log("成功连接到 Kafka 集群！");
                try {
                    if (producer == null) {
                        initializeProducer();
                    }
                    displayClusterMetadata();
                    onConnectionStateChanged.accept(true);
                } catch (ExecutionException | InterruptedException e) {
                    log("获取集群元数据失败: " + e.getMessage());
                    onConnectionStateChanged.accept(false);
                }
            });
        });

        connectTask.setOnFailed(event -> {
            javafx.application.Platform.runLater(() -> {
                Throwable e = connectTask.getException();
                log("连接失败: " + e.getMessage());
                onConnectionStateChanged.accept(false);
            });
        });

        new Thread(connectTask).start();
    }

    public void disconnect() {
        if (adminClient != null) {
            log("正在断开与 Kafka 集群的连接...");

            if (producer != null) {
                producer.close(java.time.Duration.ofSeconds(5));
                producer = null;
                log("生产者已关闭。");
            }
            if (adminClient != null) {
                adminClient.close(java.time.Duration.ofSeconds(5));
                adminClient = null;
            }

            log("已成功断开连接。");
            onConnectionStateChanged.accept(false);
        } else {
            log("当前未连接到 Kafka 集群。");
        }
    }

    private void displayClusterMetadata() throws ExecutionException, InterruptedException {
        if (adminClient == null)
            return;
        org.apache.kafka.clients.admin.DescribeClusterResult describeClusterResult = adminClient.describeCluster();
        java.util.Collection<org.apache.kafka.common.Node> nodes = describeClusterResult.nodes().get();

        log("\n--- Broker 信息 ---");
        for (org.apache.kafka.common.Node node : nodes) {
            log("Broker ID: " + node.id() + ", 地址: " + node.host() + ":" + node.port());
        }
    }

    private void initializeProducer() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "1"); // 默认值

        this.producer = new KafkaProducer<>(producerProps);
    }

    public void updateProducerConfig(String acks, String batchSize, String lingerMs) {
        if (producer == null) {
            initializeProducer();
            return;
        }

        try {
            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.ACKS_CONFIG, acks);
            producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.parseInt(batchSize));
            producerProps.put(ProducerConfig.LINGER_MS_CONFIG, Integer.parseInt(lingerMs));

            // 关闭旧的生产者
            producer.close(java.time.Duration.ofSeconds(1));
            // 创建新的生产者
            this.producer = new KafkaProducer<>(producerProps);
            log("生产者配置已更新");
        } catch (NumberFormatException e) {
            log("错误: 批次大小和延迟时间必须是有效的数字。");
            showAlert("输入错误", null, "批次大小和延迟时间必须是有效的数字。");
        }
    }

    private void showAlert(String title, String header, String content) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle(title);
        alert.setHeaderText(header);
        alert.setContentText(content);
        alert.showAndWait();
    }
}

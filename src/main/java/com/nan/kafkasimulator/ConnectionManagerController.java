package com.nan.kafkasimulator;

import static com.nan.kafkasimulator.utils.Logger.log;

import java.net.URL;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.nan.kafkasimulator.manager.ConsumerGroupUIManager;
import com.nan.kafkasimulator.manager.MessageProducerManager;

import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;

public class ConnectionManagerController implements Initializable {

    @FXML
    private TextField bootstrapServersField;

    @FXML
    private Button connectButton;
    @FXML
    private Button disconnectButton;

    private AdminClient adminClient;
    private KafkaProducer<String, String> producer;
    private String bootstrapServers;
    private Consumer<Boolean> onConnectionStateChanged;

    private MessageProducerManager messageProducerManager;
    private ConsumerGroupUIManager consumerGroupUIManager;

    @Override
    public void initialize(URL arg0, ResourceBundle arg1) {
        bootstrapServersField.setText("localhost:19092");
        bootstrapServers = bootstrapServersField.getText();
    }

    public void setMessageProducerManager(MessageProducerManager messageProducerManager) {
        this.messageProducerManager = messageProducerManager;
    }

    public void setConsumerGroupUIManager(ConsumerGroupUIManager consumerGroupUIManager) {
        this.consumerGroupUIManager = consumerGroupUIManager;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setOnConnectionStateChanged(Consumer<Boolean> onConnectionStateChanged) {
        this.onConnectionStateChanged = onConnectionStateChanged;
    }

    public void setStatusConnected(boolean connected) {
        connectButton.setDisable(connected);
        disconnectButton.setDisable(!connected);
    }

    @FXML
    protected void onConnectButtonClick() {
        connect();
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

    @FXML
    protected void onDisconnectButtonClick() {
        if (messageProducerManager != null) {
            messageProducerManager.cleanup();
        }

        if (consumerGroupUIManager != null) {
            consumerGroupUIManager.cleanup();
        }

        disconnect();
    }

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }

    public AdminClient getAdminClient() {
        return adminClient;
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

}

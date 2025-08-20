package com.nan.kafkasimulator;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MainController implements Initializable {

    @FXML
    private TextField bootstrapServersField;
    @FXML
    private Button connectButton;
    @FXML
    private TextArea logArea;
    @FXML
    private TextField newTopicNameField;
    @FXML
    private TextField numPartitionsField;
    @FXML
    private TextField replicationFactorField;
    @FXML
    private ListView<String> topicsListView;
    @FXML
    private ComboBox<String> producerTopicComboBox;
    @FXML
    private TextField producerKeyField;
    @FXML
    private TextArea producerValueArea;
    @FXML
    private ChoiceBox<String> acksChoiceBox;
    @FXML
    private TextField batchSizeField;
    @FXML
    private TextField lingerMsField;

    private AdminClient adminClient;

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        bootstrapServersField.setText("localhost:19092");
        // 初始化 acks 选择框
        acksChoiceBox.getItems().addAll("all", "1", "0");
        acksChoiceBox.setValue("1"); // 设置默认值

        // 当用户点击连接按钮后，更新Topic列表
        connectButton.setOnAction(event -> {
            onConnectButtonClick();
            // 在连接成功后自动刷新Topic列表，并填充到ComboBox中
            try {
                // 等待连接完成，这是为了确保adminClient可用
                Thread.sleep(1000);
                refreshTopicsListAndComboBox();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    @FXML
    protected void onConnectButtonClick() {
        String bootstrapServers = bootstrapServersField.getText();
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            appendToLog("错误: 请输入 Kafka 集群地址。");
            return;
        }

        appendToLog("正在尝试连接到 Kafka 集群: " + bootstrapServers);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try {
            if (adminClient != null) {
                adminClient.close();
            }
            adminClient = AdminClient.create(props);

            // 验证连接并获取元数据
            displayClusterMetadata();
            appendToLog("成功连接到 Kafka 集群！");

            Thread.sleep(1000);
            // 刷新 Topic 列表
            refreshTopicsListAndComboBox();
        } catch (Exception e) {
            appendToLog("连接失败: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
        }
    }

    private void displayClusterMetadata() throws ExecutionException, InterruptedException {
        if (adminClient == null)
            return;
        DescribeClusterResult describeClusterResult = adminClient.describeCluster();
        Collection<Node> nodes = describeClusterResult.nodes().get();

        appendToLog("\n--- Broker 信息 ---");
        for (Node node : nodes) {
            appendToLog("Broker ID: " + node.id() + ", 地址: " + node.host() + ":" + node.port());
        }
    }

    @FXML
    protected void onRefreshTopicsButtonClick() {
        refreshTopicsListAndComboBox();
    }

    private void refreshTopicsListAndComboBox() {
        if (adminClient == null) {
            appendToLog("错误: 请先连接到 Kafka 集群。");
            return;
        }

        appendToLog("正在刷新 Topic 列表...");
        try {
            Set<String> topicNames = adminClient.listTopics().names().get();

            Platform.runLater(() -> {
                // 更新 ListView
                topicsListView.getItems().clear();
                topicsListView.getItems().addAll(topicNames);

                // 更新 ComboBox
                producerTopicComboBox.getItems().clear();
                producerTopicComboBox.getItems().addAll(topicNames);

                appendToLog("Topic 列表刷新成功。");
            });
        } catch (ExecutionException | InterruptedException e) {
            appendToLog("刷新 Topic 列表失败: " + e.getMessage());
        }
    }

    @FXML
    protected void onSendButtonClick() {
        if (adminClient == null) {
            appendToLog("错误: 请先连接到 Kafka 集群。");
            return;
        }

        String topicName = producerTopicComboBox.getValue();
        if (topicName == null || topicName.isEmpty()) {
            appendToLog("错误: 请选择一个 Topic。");
            return;
        }

        String key = producerKeyField.getText();
        String value = producerValueArea.getText();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersField.getText());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, acksChoiceBox.getValue());
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.parseInt(batchSizeField.getText()));
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, Integer.parseInt(lingerMsField.getText()));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

            appendToLog("正在发送消息...");

            // 使用 Callback 来处理发送结果，这是推荐的方式
            producer.send(record, (metadata, exception) -> {
                // 确保在 JavaFX UI 线程上更新界面
                Platform.runLater(() -> {
                    if (exception == null) {
                        appendToLog("消息发送成功！");
                        appendToLog("  - Topic: " + metadata.topic());
                        appendToLog("  - 分区: " + metadata.partition());
                        appendToLog("  - 偏移量: " + metadata.offset());
                    } else {
                        appendToLog("消息发送失败: " + exception.getMessage());
                    }
                });
            });

        } catch (Exception e) {
            appendToLog("消息发送失败: " + e.getMessage());
        }
    }

    @FXML
    protected void onStartConsumerButtonClick() {
        // TODO: 在这里添加启动消费者的逻辑
        appendToLog("启动消费者按钮被点击，功能待实现。");
    }

    @FXML
    protected void onCreateTopicButtonClick() {
        if (adminClient == null) {
            appendToLog("错误: 请先连接到 Kafka 集群。");
            return;
        }

        String topicName = newTopicNameField.getText();
        if (topicName == null || topicName.trim().isEmpty()) {
            appendToLog("错误: Topic 名称不能为空。");
            return;
        }

        int numPartitions = Integer.parseInt(numPartitionsField.getText());
        short replicationFactor = Short.parseShort(replicationFactorField.getText());

        NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);

        try {
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            appendToLog("成功创建 Topic: " + topicName);
            // 立即刷新列表以显示新创建的 Topic
            refreshTopicsListAndComboBox();
        } catch (ExecutionException | InterruptedException e) {
            appendToLog("创建 Topic 失败: " + e.getCause().getMessage());
        }
    }

    @FXML
    protected void onDeleteTopicButtonClick() {
        if (adminClient == null) {
            appendToLog("错误: 请先连接到 Kafka 集群。");
            return;
        }

        String selectedTopic = topicsListView.getSelectionModel().getSelectedItem();
        if (selectedTopic == null || selectedTopic.trim().isEmpty()) {
            appendToLog("错误: 请选择一个 Topic 来删除。");
            return;
        }

        try {
            adminClient.deleteTopics(Collections.singleton(selectedTopic)).all().get();
            appendToLog("成功删除 Topic: " + selectedTopic);
            // 立即刷新列表以移除已删除的 Topic
            refreshTopicsListAndComboBox();
        } catch (ExecutionException | InterruptedException e) {
            appendToLog("删除 Topic 失败: " + e.getCause().getMessage());
        }
    }

    private void appendToLog(String message) {
        Platform.runLater(() -> {
            logArea.appendText(message + "\n");
        });
    }
}
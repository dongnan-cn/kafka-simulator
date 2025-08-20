package com.nan.kafkasimulator;

import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import jakarta.annotation.PreDestroy;

import java.net.URL;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MainController implements Initializable {

    @FXML
    private TextField bootstrapServersField;
    @FXML
    private Button connectButton;
    @FXML
    private Button disconnectButton; // 新增: 断开连接按钮
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
    @FXML
    private TextField consumerGroupIdField;
    @FXML
    private TextField consumerTopicField;
    @FXML
    private ChoiceBox<String> autoCommitChoiceBox;
    @FXML
    private Button onStartConsumerButtonClick;
    @FXML
    private TextArea consumerMessagesArea;
    @FXML
    private Button onCreateTopicButtonClick;
    @FXML
    private Button onRefreshTopicsButtonClick;
    @FXML
    private Button onDeleteTopicButtonClick;
    @FXML
    private Button onSendButtonClick;
    @FXML
    private Button onStopConsumerButtonClick;

    private AdminClient adminClient;
    private volatile boolean isConsuming = false;
    private Thread consumerThread;

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        bootstrapServersField.setText("localhost:19092");

        acksChoiceBox.getItems().addAll("all", "1", "0");
        acksChoiceBox.setValue("1");

        autoCommitChoiceBox.getItems().addAll("true", "false");
        autoCommitChoiceBox.setValue("true");

        setAllControlsDisable(true);
        // 初始状态，连接按钮可用，断开按钮不可用
        connectButton.setDisable(false);
        disconnectButton.setDisable(true);
    }

    private void setAllControlsDisable(boolean disable) {
        newTopicNameField.setDisable(disable);
        numPartitionsField.setDisable(disable);
        replicationFactorField.setDisable(disable);
        onCreateTopicButtonClick.setDisable(disable);
        onRefreshTopicsButtonClick.setDisable(disable);
        onDeleteTopicButtonClick.setDisable(disable);
        topicsListView.setDisable(disable);
        producerTopicComboBox.setDisable(disable);
        producerKeyField.setDisable(disable);
        producerValueArea.setDisable(disable);
        acksChoiceBox.setDisable(disable);
        batchSizeField.setDisable(disable);
        lingerMsField.setDisable(disable);
        onSendButtonClick.setDisable(disable);
        consumerGroupIdField.setDisable(disable);
        consumerTopicField.setDisable(disable);
        autoCommitChoiceBox.setDisable(disable);
        onStartConsumerButtonClick.setDisable(disable);
        onStopConsumerButtonClick.setDisable(disable);
    }

    @FXML
    protected void onConnectButtonClick() {
        String bootstrapServers = bootstrapServersField.getText();
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            showAlert("连接错误", null, "请输入 Kafka 集群地址。");
            return;
        }

        connectButton.setDisable(true);
        appendToLog("正在尝试连接到 Kafka 集群: " + bootstrapServers);

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
            Platform.runLater(() -> {
                appendToLog("成功连接到 Kafka 集群！");
                try {
                    displayClusterMetadata();
                    refreshTopicsListAndComboBox();
                    // 连接成功后，启用所有UI控件并切换按钮状态
                    setAllControlsDisable(false);
                    connectButton.setDisable(true);
                    disconnectButton.setDisable(false);
                } catch (ExecutionException | InterruptedException e) {
                    appendToLog("获取集群元数据失败: " + e.getMessage());
                    setAllControlsDisable(true);
                }
            });
        });

        connectTask.setOnFailed(event -> {
            Platform.runLater(() -> {
                Throwable e = connectTask.getException();
                appendToLog("连接失败: " + e.getMessage());
                setAllControlsDisable(true);
                connectButton.setDisable(false);
            });
        });

        new Thread(connectTask).start();
    }

    private void showAlert(String title, String header, String content) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle(title);
        alert.setHeaderText(header);
        alert.setContentText(content);
        alert.showAndWait();
    }

    // 新增: 断开连接的事件处理方法
    @FXML
    protected void onDisconnectButtonClick() {
        if (adminClient != null) {
            appendToLog("正在断开与 Kafka 集群的连接...");
            onStopConsumerButtonClick(); // 停止所有消费者
            adminClient.close(Duration.ofSeconds(10)); // 优雅关闭
            adminClient = null;
            appendToLog("已成功断开连接。");
            // 断开后，禁用所有控件并切换按钮状态
            setAllControlsDisable(true);
            connectButton.setDisable(false);
            disconnectButton.setDisable(true);
        } else {
            appendToLog("当前未连接到 Kafka 集群。");
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
                topicsListView.getItems().clear();
                topicsListView.getItems().addAll(topicNames);
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

        try {
            producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.parseInt(batchSizeField.getText()));
            producerProps.put(ProducerConfig.LINGER_MS_CONFIG, Integer.parseInt(lingerMsField.getText()));
        } catch (NumberFormatException e) {
            appendToLog("错误: 批次大小和延迟时间必须是有效的数字。");
            return;
        }

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

            appendToLog("正在发送消息...");
            producer.send(record, (metadata, exception) -> {
                Platform.runLater(() -> {
                    if (exception == null) {
                        appendToLog("消息发送成功！");
                        appendToLog("  - Topic: " + metadata.topic());
                        appendToLog("  - 分区: " + metadata.partition());
                        appendToLog("  - 偏移量: " + metadata.offset());
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
        if (adminClient == null) {
            appendToLog("错误: 请先连接到 Kafka 集群。");
            return;
        }
        if (isConsuming) {
            appendToLog("错误: 消费者已在运行。");
            return;
        }

        String groupId = consumerGroupIdField.getText();

        if (groupId == null || groupId.trim().isEmpty()) {
            appendToLog("错误: 消费者组 ID 不能为空。");
            return;
        }

        String topicNamesStr = consumerTopicField.getText();
        if (topicNamesStr == null || topicNamesStr.trim().isEmpty()) {
            appendToLog("错误: 订阅 Topic 不能为空。");
            return;
        }

        List<String> topicNames = Arrays.asList(topicNamesStr.split("\\s*,\\s*"));
        if (topicNames.isEmpty()) {
            appendToLog("错误: 订阅 Topic 不能为空。");
            return;
        }

        appendToLog("正在启动消费者...");
        onStartConsumerButtonClick.setDisable(true);
        onStopConsumerButtonClick.setDisable(false);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersField.getText());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.valueOf(autoCommitChoiceBox.getValue()));
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(topicNames);
        startPollingThread(consumer);
    }

    private void startPollingThread(KafkaConsumer<String, String> consumer) {
        isConsuming = true;
        consumerThread = new Thread(() -> {
            try {
                while (isConsuming) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        String message = String.format("收到消息: Topic = %s, 分区 = %d, 偏移量 = %d, Key = %s, Value = %s%n",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                        Platform.runLater(() -> consumerMessagesArea.appendText(message));
                    }
                }
            } catch (Exception e) {
                if (isConsuming) {
                    Platform.runLater(() -> appendToLog("消费者线程出错: " + e.getMessage()));
                }
            } finally {
                consumer.close();
                isConsuming = false;
                Platform.runLater(() -> {
                    appendToLog("消费者已停止。");
                    onStartConsumerButtonClick.setDisable(false);
                    onStopConsumerButtonClick.setDisable(true);
                });
            }
        });

        consumerThread.setDaemon(true);
        consumerThread.start();
        appendToLog("消费者已成功启动！");
    }

    @FXML
    protected void onStopConsumerButtonClick() {
        if (!isConsuming) {
            appendToLog("错误: 消费者未运行。");
            return;
        }
        isConsuming = false;
        if (consumerThread != null) {
            consumerThread.interrupt();
        }
        appendToLog("正在尝试停止消费者...");
        onStopConsumerButtonClick.setDisable(true);
        onStartConsumerButtonClick.setDisable(false);
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

        try {
            int numPartitions = Integer.parseInt(numPartitionsField.getText());
            short replicationFactor = Short.parseShort(replicationFactorField.getText());

            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            appendToLog("成功创建 Topic: " + topicName);
            refreshTopicsListAndComboBox();
        } catch (NumberFormatException e) {
            appendToLog("错误: 分区数和副本因子必须是有效的数字。");
            // 也可以使用 Alert 弹出对话框
            showAlert("输入错误", null, "分区数和副本因子必须是有效的数字。");
        } catch (ExecutionException | InterruptedException e) {
            appendToLog("创建 Topic 失败: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
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
            refreshTopicsListAndComboBox();
        } catch (ExecutionException | InterruptedException e) {
            appendToLog("删除 Topic 失败: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
        }
    }

    private void appendToLog(String message) {
        Platform.runLater(() -> {
            logArea.appendText(message + "\n");
            logArea.setScrollTop(Double.MAX_VALUE);
        });
    }

    @PreDestroy
    public void cleanup() {
        appendToLog("正在关闭应用程序...");
        onStopConsumerButtonClick();
        if (adminClient != null) {
            adminClient.close(Duration.ofSeconds(10));
        }
        appendToLog("所有资源已释放。");
    }
}
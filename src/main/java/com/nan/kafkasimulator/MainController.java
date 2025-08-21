package com.nan.kafkasimulator;

import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.layout.VBox;

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
import java.util.stream.Collectors;

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
    private VBox topicCheckBoxContainer;
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
    @FXML
    private Button onShowPartitionAssignmentButtonClick; // 新增: 显示分区分配按钮
    @FXML
    private TextArea partitionAssignmentArea; // 新增: 分区分配结果显示区域

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
        topicCheckBoxContainer.setDisable(disable);
        autoCommitChoiceBox.setDisable(disable);
        onStartConsumerButtonClick.setDisable(disable);
        onStopConsumerButtonClick.setDisable(!disable);
        onShowPartitionAssignmentButtonClick.setDisable(disable);
        partitionAssignmentArea.setDisable(disable);
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
                createConsumerTopicCheckBoxes(topicNames);
                appendToLog("Topic 列表刷新成功。");
            });
        } catch (ExecutionException | InterruptedException e) {
            appendToLog("刷新 Topic 列表失败: " + e.getMessage());
        }
    }

    private void createConsumerTopicCheckBoxes(Set<String> topicNames) {
        topicCheckBoxContainer.getChildren().clear();
        topicNames.stream().sorted().forEach(topicName -> {
            CheckBox checkBox = new CheckBox(topicName);
            topicCheckBoxContainer.getChildren().add(checkBox);
        });
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

        List<String> topicNames = topicCheckBoxContainer.getChildren().stream()
            .filter(node -> node instanceof CheckBox)
            .map(node -> (CheckBox) node)
            .filter(CheckBox::isSelected)
            .map(CheckBox::getText)
            .collect(Collectors.toList());

        if (topicNames.isEmpty()) {
            appendToLog("错误: 订阅 Topic 不能为空。");
            return;
        }

        appendToLog("正在启动消费者...");
        onStartConsumerButtonClick.setDisable(true);
        onStopConsumerButtonClick.setDisable(false);
        // 开启消费后不能更改消费的主题
        topicCheckBoxContainer.setDisable(true);
        consumerGroupIdField.setDisable(true);

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
                    try {
                        // 使用较短的轮询时间，以便更快响应停止请求
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                        for (ConsumerRecord<String, String> record : records) {
                            String message = String.format(
                                    "收到消息: Topic = %s, 分区 = %d, 偏移量 = %d, Key = %s, Value = %s%n",
                                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
                            Platform.runLater(() -> consumerMessagesArea.appendText(message));
                        }
                    } catch (org.apache.kafka.common.errors.InterruptException e) {
                        // 线程被中断，优雅地退出循环
                        System.out.println("消费者轮询被中断，准备退出");
                        break;
                    } catch (Exception e) {
                        // 处理其他异常
                        if (isConsuming) {
                            Platform.runLater(() -> appendToLog("消费者轮询出错: " + e.getMessage()));
                            // 短暂暂停避免异常情况下的快速循环
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException ie) {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                }
                System.out.println("消费者线程已停止");
            } catch (Exception e) {
                // 捕获外层循环中可能发生的异常
                if (isConsuming) {
                    Platform.runLater(() -> appendToLog("消费者线程出错: " + e.getMessage()));
                }
            } finally {
                try {
                    // 尝试优雅地关闭消费者
                    consumer.close(Duration.ofSeconds(5));
                } catch (Exception e) {
                    Platform.runLater(() -> appendToLog("关闭消费者时出错: " + e.getMessage()));
                }

                isConsuming = false;
                Platform.runLater(() -> {
                    appendToLog("消费者已停止。");
                    onStartConsumerButtonClick.setDisable(false);
                    // 消费停止后可以重新选择消费的主题
                    topicCheckBoxContainer.setDisable(false);
                    System.out.println("消费停止后可以重新选择消费的主题");
                    consumerGroupIdField.setDisable(false);
                    onShowPartitionAssignmentButtonClick.setDisable(false);
                });
            }
        });

        consumerThread.setDaemon(true);
        consumerThread.start();
        appendToLog("消费者已成功启动！");
    }

    @FXML
    protected void onShowPartitionAssignmentButtonClick() {
        if (adminClient == null) {
            appendToLog("错误: 请先连接到 Kafka 集群。");
            return;
        }
        String groupId = consumerGroupIdField.getText();
        if (groupId == null || groupId.trim().isEmpty()) {
            appendToLog("错误: 消费者组 ID 不能为空。");
            return;
        }

        appendToLog("正在查询消费者组 '" + groupId + "' 的分区分配情况...");
        partitionAssignmentArea.clear();

        Task<Map<String, List<String>>> assignmentTask = new Task<>() {
            @Override
            protected Map<String, List<String>> call() throws Exception {
                DescribeConsumerGroupsResult result = adminClient
                        .describeConsumerGroups(Collections.singleton(groupId));
                ConsumerGroupDescription description = result.describedGroups().get(groupId).get();

                Map<String, List<String>> assignments = new HashMap<>();
                if (description.members() != null) {
                    for (MemberDescription member : description.members()) {
                        String memberId = member.consumerId();
                        if (memberId == null || memberId.isEmpty()) {
                            memberId = member.host() + "/" + member.clientId();
                        }

                        List<String> assignedPartitions = member.assignment().topicPartitions().stream()
                                .map(tp -> tp.topic() + "-" + tp.partition())
                                .collect(Collectors.toList());
                        assignments.put(memberId, assignedPartitions);
                    }
                }
                return assignments;
            }
        };

        assignmentTask.setOnSucceeded(event -> {
            Map<String, List<String>> assignments = assignmentTask.getValue();
            Platform.runLater(() -> {
                StringBuilder sb = new StringBuilder();
                sb.append("消费者组 '").append(groupId).append("' 的分区分配:\n");
                if (assignments.isEmpty()) {
                    sb.append("当前消费者组没有活跃成员或分配的分区。");
                } else {
                    assignments.forEach((memberId, partitions) -> {
                        sb.append("-> 消费者 ").append(memberId).append(":\n");
                        if (partitions.isEmpty()) {
                            sb.append("   - 未分配任何分区。\n");
                        } else {
                            partitions.forEach(p -> sb.append("   - ").append(p).append("\n"));
                        }
                    });
                }
                partitionAssignmentArea.setText(sb.toString());
                appendToLog("分区分配查询成功。");
            });
        });

        assignmentTask.setOnFailed(event -> {
            Throwable e = assignmentTask.getException();
            Platform.runLater(() -> {
                appendToLog("获取分区分配失败: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
                partitionAssignmentArea.setText(
                        "获取分区分配失败: " + (e.getCause() != null ? e.getCause().getMessage() : e.getCause().getMessage()));
            });
        });

        new Thread(assignmentTask).start();
    }

    @FXML
    protected void onStopConsumerButtonClick() {
        if (!isConsuming) {
            appendToLog("错误: 消费者未运行。");
            return;
        }

        // 只设置标志位，不直接中断线程
        isConsuming = false;
        appendToLog("正在尝试停止消费者...");
        onStopConsumerButtonClick.setDisable(true);
        System.out.println("正在尝试停止消费者...");

        // 给消费者线程一些时间来优雅地退出
        new Thread(() -> {
            try {
                // 等待最多5秒让消费者线程自然结束
                if (consumerThread != null) {
                    consumerThread.join(5000);
                    // 如果线程仍在运行，才强制中断
                    if (consumerThread.isAlive()) {
                        consumerThread.interrupt();
                    }
                }
            } catch (InterruptedException e) {
                Platform.runLater(() -> appendToLog("等待消费者停止时被中断: " + e.getMessage()));
            }
        }).start();
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
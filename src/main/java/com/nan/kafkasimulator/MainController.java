package com.nan.kafkasimulator;

import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
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
    private Button disconnectButton;
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
    private TextArea consumerMessagesArea; // 在FXML中动态添加，这里保留以便于在启动时引用
    @FXML
    private TextArea partitionAssignmentArea; // 在FXML中动态添加，这里保留以便于在启动时引用
    @FXML
    private Button onCreateTopicButtonClick;
    @FXML
    private Button onRefreshTopicsButtonClick;
    @FXML
    private Button onDeleteTopicButtonClick;
    @FXML
    private Button onSendButtonClick;

    // 新增：与消费者组管理Tab相关的UI元素
    @FXML
    private TabPane consumerTabPane;
    @FXML
    private ChoiceBox<String> autoCommitChoiceBox;

    private AdminClient adminClient;
    // 移除旧的单消费者线程和状态管理
    // private volatile boolean isConsuming = false;
    // private Thread consumerThread;

    // 新增：用于管理所有消费者组实例的Map
    private final Map<String, ConsumerGroupManager> activeConsumerGroups = new HashMap<>();
    private final Map<String, Tab> consumerGroupTabs = new HashMap<>();

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        bootstrapServersField.setText("localhost:19092");

        acksChoiceBox.getItems().addAll("all", "1", "0");
        acksChoiceBox.setValue("1");

        setAllControlsDisable(true);
        // 初始状态，连接按钮可用，断开按钮不可用
        connectButton.setDisable(false);
        disconnectButton.setDisable(true);
        autoCommitChoiceBox.getItems().addAll("true", "false");
        autoCommitChoiceBox.setValue("true");
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

        // 禁用消费者TabPane，因为它里面的内容现在是动态管理的
        consumerTabPane.setDisable(disable);
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

    @FXML
    protected void onDisconnectButtonClick() {
        if (adminClient != null) {
            appendToLog("正在断开与 Kafka 集群的连接...");
            // 停止所有消费者组
            activeConsumerGroups.values().forEach(ConsumerGroupManager::stopAll);
            activeConsumerGroups.clear();

            adminClient.close(Duration.ofSeconds(10));
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
                // 确保所有消费者组Tab中的Topic列表都被刷新
                updateAllConsumerTopics(new ArrayList<>(topicNames));
                appendToLog("Topic 列表刷新成功。");
            });
        } catch (ExecutionException | InterruptedException e) {
            appendToLog("刷新 Topic 列表失败: " + e.getMessage());
        }
    }

    private void updateAllConsumerTopics(List<String> topicNames) {
        // 获取“新增消费者组”这个Tab
        Tab createNewTab = consumerTabPane.getTabs().get(0);

        // 直接通过ID查找 VBox 节点，并将其转换为 VBox
        VBox topicContainer = (VBox) createNewTab.getContent().lookup("#topicCheckBoxContainer");

        // 添加一个检查以防万一
        if (topicContainer == null) {
            appendToLog("错误：在 '新增消费者组' 选项卡中找不到 topicCheckBoxContainer VBox。");
            return;
        }

        // 清空现有的复选框
        topicContainer.getChildren().clear();

        // 为每个 Topic 创建一个新的复选框并添加到 VBox 中
        topicNames.stream().sorted().forEach(topicName -> {
            CheckBox checkBox = new CheckBox(topicName);
            topicContainer.getChildren().add(checkBox);
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
            showAlert("输入错误", null, "批次大小和延迟时间必须是有效的数字。");
            return;
        }

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

            appendToLog("正在发送消息...");
            producer.send(record, (metadata, exception) -> {
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
    @SuppressWarnings("unchecked")
    protected void onStartConsumerButtonClick() {
        // 从“新增消费者组”Tab中获取UI元素
        Tab createNewTab = consumerTabPane.getTabs().get(0);
        TextField groupIdField = (TextField) createNewTab.getContent().lookup("#consumerGroupIdField");
        VBox topicContainer = (VBox) createNewTab.getContent().lookup("#topicCheckBoxContainer");
        ChoiceBox<String> autoCommitBox = (ChoiceBox<String>) createNewTab.getContent().lookup("#autoCommitChoiceBox");

        String groupId = groupIdField.getText();
        if (groupId == null || groupId.trim().isEmpty()) {
            appendToLog("错误: 消费者组 ID 不能为空。");
            return;
        }
        if (activeConsumerGroups.containsKey(groupId)) {
            appendToLog("错误: 消费者组 '" + groupId + "' 已存在。");
            return;
        }

        List<String> topicNames = topicContainer.getChildren().stream()
                .filter(node -> node instanceof CheckBox)
                .map(node -> (CheckBox) node)
                .filter(CheckBox::isSelected)
                .map(CheckBox::getText)
                .collect(Collectors.toList());

        if (topicNames.isEmpty()) {
            appendToLog("错误: 订阅 Topic 不能为空。");
            return;
        }

        // 动态创建新的Tab来显示这个消费者组
        Tab newTab = new Tab(groupId);
        VBox content = new VBox();
        content.setSpacing(10.0);
        content.setPadding(new javafx.geometry.Insets(10.0));

        // 为新Tab添加UI元素
        TextArea messagesArea = new TextArea();
        messagesArea.setEditable(false);
        messagesArea.setPrefHeight(200.0);
        messagesArea.setPrefWidth(200.0);
        messagesArea.setId("consumerMessagesArea_" + groupId);

        TextArea partitionsArea = new TextArea();
        partitionsArea.setEditable(false);
        partitionsArea.setPrefHeight(200.0);
        partitionsArea.setPrefWidth(200.0);
        partitionsArea.setId("partitionAssignmentArea_" + groupId);

        Button stopButton = new Button("停止消费者组");
        stopButton.setPrefWidth(Double.MAX_VALUE);
        Button addConsumerButton = new Button("添加一个消费者实例");
        addConsumerButton.setPrefWidth(Double.MAX_VALUE);
        Button showAssignmentButton = new Button("显示分区分配");
        showAssignmentButton.setPrefWidth(Double.MAX_VALUE);

        content.getChildren().addAll(
                new Label("收到的消息"),
                messagesArea,
                new Label("分区分配"),
                partitionsArea,
                new HBox(5.0, addConsumerButton, showAssignmentButton),
                stopButton);
        newTab.setContent(content);

        // 创建新的ConsumerGroupManager实例
        ConsumerGroupManager manager = new ConsumerGroupManager(
                groupId,
                topicNames,
                Boolean.valueOf(autoCommitBox.getValue()),
                bootstrapServersField.getText(),
                messagesArea,
                partitionsArea,
                logArea);
        activeConsumerGroups.put(groupId, manager);
        consumerGroupTabs.put(groupId, newTab);

        // 绑定按钮事件
        stopButton.setOnAction(event -> manager.stopAll());
        addConsumerButton.setOnAction(event -> manager.startNewConsumerInstance());
        showAssignmentButton.setOnAction(event -> manager.showPartitionAssignments(adminClient));

        // 启动消费者组
        manager.start(1);

        // 添加新Tab并切换到它
        consumerTabPane.getTabs().add(newTab);
        consumerTabPane.getSelectionModel().select(newTab);

        appendToLog("已启动消费者组 '" + groupId + "'。");
        // 清空模板Tab的输入框，以便于创建新的消费者组
        groupIdField.clear();
        topicContainer.getChildren().forEach(node -> {
            if (node instanceof CheckBox) {
                ((CheckBox) node).setSelected(false);
            }
        });
    }

    // 移除 onStopConsumerButtonClick 和 onShowPartitionAssignmentButtonClick
    // 这些功能现在由 ConsumerGroupManager 实例管理，并绑定到动态创建的按钮上

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
        activeConsumerGroups.values().forEach(ConsumerGroupManager::stopAll);
        if (adminClient != null) {
            adminClient.close(Duration.ofSeconds(10));
        }
        appendToLog("所有资源已释放。");
    }
}
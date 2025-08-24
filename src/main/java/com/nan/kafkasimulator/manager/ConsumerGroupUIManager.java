package com.nan.kafkasimulator.manager;

import javafx.scene.control.*;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import com.nan.kafkasimulator.ConsumerGroupManager;

import java.util.List;
import java.util.Map;
import static com.nan.kafkasimulator.utils.Logger.log;

/**
 * 负责管理消费者组UI的类，包括创建消费者组Tab、更新Topic列表等操作
 */
public class ConsumerGroupUIManager {
    private final TabPane consumerTabPane;
    private final String bootstrapServers;
    private final Map<String, ConsumerGroupManager> activeConsumerGroups;
    private final Map<String, Tab> consumerGroupTabs;
    private org.apache.kafka.clients.admin.AdminClient adminClient;

    public ConsumerGroupUIManager(TabPane consumerTabPane, String bootstrapServers,
                                Map<String, ConsumerGroupManager> activeConsumerGroups,
                                Map<String, Tab> consumerGroupTabs) {
        this.consumerTabPane = consumerTabPane;
        this.bootstrapServers = bootstrapServers;
        this.activeConsumerGroups = activeConsumerGroups;
        this.consumerGroupTabs = consumerGroupTabs;
    }

    public void setAdminClient(org.apache.kafka.clients.admin.AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    public void updateAllConsumerTopics(List<String> topicNames) {
        // 获取"新增消费者组"这个Tab
        Tab createNewTab = consumerTabPane.getTabs().get(0);

        // 直接通过ID查找 VBox 节点，并将其转换为 VBox
        VBox topicContainer = (VBox) createNewTab.getContent().lookup("#topicCheckBoxContainer");

        // 添加一个检查以防万一
        if (topicContainer == null) {
            log("错误：在 '新增消费者组' 选项卡中找不到 topicCheckBoxContainer VBox。");
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

    public void createConsumerGroup() {
        // 从"新增消费者组"Tab中获取UI元素
        Tab createNewTab = consumerTabPane.getTabs().get(0);
        TextField groupIdField = (TextField) createNewTab.getContent().lookup("#consumerGroupIdField");
        VBox topicContainer = (VBox) createNewTab.getContent().lookup("#topicCheckBoxContainer");
        @SuppressWarnings("unchecked")
        ChoiceBox<String> autoCommitBox = (ChoiceBox<String>) createNewTab.getContent().lookup("#autoCommitChoiceBox");

        String groupId = groupIdField.getText();
        if (groupId == null || groupId.trim().isEmpty()) {
            log("错误: 消费者组 ID 不能为空。");
            return;
        }
        if (activeConsumerGroups.containsKey(groupId)) {
            log("错误: 消费者组 '" + groupId + "' 已存在。");
            return;
        }

        List<String> topicNames = topicContainer.getChildren().stream()
                .filter(node -> node instanceof CheckBox)
                .map(node -> (CheckBox) node)
                .filter(CheckBox::isSelected)
                .map(CheckBox::getText)
                .collect(java.util.stream.Collectors.toList());

        if (topicNames.isEmpty()) {
            log("错误: 订阅 Topic 不能为空。");
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

        HBox buttonBox = new HBox();
        Button stopButton = new Button("停止消费者组");
        Button addConsumerButton = new Button("添加一个消费者实例");
        Button showAssignmentButton = new Button("显示分区分配");
        showAssignmentButton.setPrefWidth(Double.MAX_VALUE);
        Button resumeButton = new Button("恢复消费者组");
        buttonBox.getChildren().addAll(addConsumerButton, stopButton, resumeButton);
        buttonBox.setSpacing(10); // 设置按钮之间的间距为 10 像素
        buttonBox.setAlignment(javafx.geometry.Pos.CENTER);

        content.getChildren().addAll(
                new Label("收到的消息"),
                messagesArea,
                new Label("分区分配"),
                partitionsArea,
                showAssignmentButton,
                buttonBox);
        newTab.setContent(content);

        // 创建新的ConsumerGroupManager实例
        ConsumerGroupManager manager = new ConsumerGroupManager(
                groupId,
                topicNames,
                Boolean.valueOf(autoCommitBox.getValue()),
                bootstrapServers,
                messagesArea,
                partitionsArea);
        activeConsumerGroups.put(groupId, manager);
        consumerGroupTabs.put(groupId, newTab);

        // 绑定按钮事件
        stopButton.setOnAction(event -> {
            manager.pauseAll();
            stopButton.setDisable(true);
            resumeButton.setDisable(false);
        });
        resumeButton.setOnAction(event -> {
            manager.resumeAll();
            resumeButton.setDisable(true);
            stopButton.setDisable(false);
        });
        addConsumerButton.setOnAction(event -> manager.startNewConsumerInstance());
        showAssignmentButton.setOnAction(event -> {
            if (adminClient != null) {
                manager.showPartitionAssignments(adminClient);
            } else {
                log("错误: AdminClient未初始化，请确保已连接到Kafka集群");
            }
        });

        // 启动消费者组
        manager.start(1);
        resumeButton.setDisable(true);

        // 添加新Tab并切换到它
        consumerTabPane.getTabs().add(newTab);
        consumerTabPane.getSelectionModel().select(newTab);

        log("已启动消费者组 '" + groupId + "'。");
        // 清空模板Tab的输入框，以便于创建新的消费者组
        groupIdField.clear();
        topicContainer.getChildren().forEach(node -> {
            if (node instanceof CheckBox) {
                ((CheckBox) node).setSelected(false);
            }
        });
    }

    public void cleanup() {
        // 停止所有消费者组
        activeConsumerGroups.values().forEach(ConsumerGroupManager::stopAll);
        activeConsumerGroups.clear();

        // 移除所有消费者组Tab
        if (consumerTabPane.getTabs().size() > 1) {
            consumerTabPane.getTabs().remove(1, consumerTabPane.getTabs().size());
        }
    }
}

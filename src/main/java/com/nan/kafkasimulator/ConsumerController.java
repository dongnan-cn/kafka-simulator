package com.nan.kafkasimulator;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.layout.TilePane;
import javafx.scene.layout.VBox;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import static com.nan.kafkasimulator.utils.Logger.log;

public class ConsumerController implements Initializable {

    @FXML
    private TextField consumerGroupIdField;
    @FXML
    private VBox topicCheckBoxContainer;
    @FXML
    private ChoiceBox<String> autoCommitChoiceBox;
    @FXML
    private ListView<String> consumerGroupListView;
    @FXML
    private TilePane consumerGroupsDisplayArea;
    @FXML
    private Button removeSelectedButton;
    @FXML
    private Button refreshListButton;
    @FXML
    private TabPane consumerTabPane;

    private final Map<String, ConsumerGroupManager> activeConsumerGroups = new HashMap<>();
    private final Map<String, ConsumerGroupPanelController> consumerGroupPanels = new HashMap<>();

    private org.apache.kafka.clients.admin.AdminClient adminClient;
    private String bootstrapServers;

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        // 初始化自动提交选项
        autoCommitChoiceBox.getItems().addAll("true", "false");
        autoCommitChoiceBox.setValue("true");
        ControllerRegistry.setConsumerController(this);

        // 初始化消费者组列表
        consumerGroupListView.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
        consumerGroupListView.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            updateSelectedConsumerGroupsDisplay();
            removeSelectedButton.setDisable(consumerGroupListView.getSelectionModel().getSelectedItems().isEmpty());
        });

        // 绑定按钮事件
        removeSelectedButton.setOnAction(event -> removeSelectedConsumerGroups());
        refreshListButton.setOnAction(event -> refreshConsumerGroupsList());
    }

    // private void updateSelectedConsumerGroupsDisplay() {
    //     consumerGroupsDisplayArea.getChildren().clear();
    //     List<String> selectedGroups = consumerGroupListView.getSelectionModel().getSelectedItems();

    //     for (String groupId : selectedGroups) {
    //         Label groupLabel = new Label(groupId);
    //         groupLabel.setStyle("-fx-padding: 5; -fx-border-color: lightgray; -fx-border-radius: 3;");
    //         consumerGroupsDisplayArea.getChildren().add(groupLabel);
    //     }
    // }

    // private void refreshConsumerGroupsList() {
    //     if (adminClient == null) {
    //         log("错误: AdminClient未初始化，请确保已连接到Kafka集群");
    //         return;
    //     }

    //     try {
    //         // 获取所有消费者组
    //         List<String> groupIds = new java.util.ArrayList<>(adminClient.listConsumerGroups().all().get()
    //                 .stream()
    //                 .map(group -> group.groupId())
    //                 .collect(java.util.stream.Collectors.toList()));

    //         // 更新ListView
    //         consumerGroupListView.getItems().clear();
    //         consumerGroupListView.getItems().addAll(groupIds);

    //         log("已刷新消费者组列表");
    //     } catch (Exception e) {
    //         log("刷新消费者组列表时出错: " + e.getMessage());
    //     }
    // }

    // private void removeSelectedConsumerGroups() {
    //     List<String> selectedGroups = consumerGroupListView.getSelectionModel().getSelectedItems();

    //     for (String groupId : selectedGroups) {
    //         // 停止并移除消费者组管理器
    //         ConsumerGroupManager manager = activeConsumerGroups.remove(groupId);
    //         if (manager != null) {
    //             manager.stopAll();
    //         }

    //         // 移除对应的Tab
    //         Tab tab = consumerGroupTabs.remove(groupId);
    //         if (tab != null) {
    //             consumerTabPane.getTabs().remove(tab);
    //         }

    //         log("已移除消费者组 '" + groupId + "'");
    //     }

    //     // 清空选择并更新显示
    //     consumerGroupListView.getSelectionModel().clearSelection();
    //     updateSelectedConsumerGroupsDisplay();
    // }

    public void setControlsDisable(boolean disable) {
        // 禁用主要控件
        consumerGroupIdField.setDisable(disable);
        topicCheckBoxContainer.setDisable(disable);
        autoCommitChoiceBox.setDisable(disable);
        consumerGroupListView.setDisable(disable);
        consumerGroupsDisplayArea.setDisable(disable);
        removeSelectedButton.setDisable(disable);
        refreshListButton.setDisable(disable);
    }

    /**
     * 设置AdminClient
     * 
     * @param adminClient Kafka AdminClient
     */
    public void setAdminClient(org.apache.kafka.clients.admin.AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    /**
     * 设置Bootstrap服务器地址
     * 
     * @param bootstrapServers Kafka服务器地址
     */
    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * 更新所有消费者的Topic列表
     * 
     * @param topicNames Topic名称列表
     */
    public void updateAllConsumerTopics(List<String> topicNames) {
        // 添加一个检查以防万一
        if (topicCheckBoxContainer == null) {
            log("错误：找不到 topicCheckBoxContainer VBox。");
            return;
        }

        // 清空现有的复选框
        topicCheckBoxContainer.getChildren().clear();

        // 为每个 Topic 创建一个新的复选框并添加到 VBox 中
        topicNames.stream().sorted().forEach(topicName -> {
            CheckBox checkBox = new CheckBox(topicName);
            topicCheckBoxContainer.getChildren().add(checkBox);
        });
    }

    /**
     * 启动消费者组按钮点击事件处理
     */
    @FXML
    private void onStartConsumerButtonClick() {
        createConsumerGroup();
    }

    /**
     * 创建新的消费者组
     */
    public void createConsumerGroup() {
        // 获取UI元素
        String groupId = consumerGroupIdField.getText();
        if (groupId == null || groupId.trim().isEmpty()) {
            log("错误: 消费者组 ID 不能为空。");
            return;
        }
        if (activeConsumerGroups.containsKey(groupId)) {
            log("错误: 消费组 '" + groupId + "' 已存在。");
            return;
        }

        List<String> topicNames = topicCheckBoxContainer.getChildren().stream()
                .filter(node -> node instanceof CheckBox)
                .map(node -> (CheckBox) node)
                .filter(CheckBox::isSelected)
                .map(CheckBox::getText)
                .collect(java.util.stream.Collectors.toList());

        if (topicNames.isEmpty()) {
            log("错误: 订阅 Topic 不能为空。");
            return;
        }

        // 创建新的消费者组面板
        ConsumerGroupPanelController panelController = ConsumerGroupPanelController.create(
                groupId,
                topicNames,
                Boolean.valueOf(autoCommitChoiceBox.getValue()),
                bootstrapServers,
                adminClient);

        if (panelController == null) {
            log("错误: 创建消费者组面板失败。");
            return;
        }

        // 将面板添加到显示区域
        consumerGroupsDisplayArea.getChildren().add(panelController.getPanel());

        // 保存消费者组管理器和面板控制器
        activeConsumerGroups.put(groupId, panelController.getConsumerGroupManager());
        consumerGroupPanels.put(groupId, panelController);

        // 更新消费者组列表
        updateConsumerGroupsList();

        log("已启动消费者组 '" + groupId + "'。");
        // 清空输入框，以便于创建新的消费者组
        consumerGroupIdField.clear();
        topicCheckBoxContainer.getChildren().forEach(node -> {
            if (node instanceof CheckBox) {
                ((CheckBox) node).setSelected(false);
            }
        });
    }

    /**
     * 更新消费者组列表
     */
    private void updateConsumerGroupsList() {
        consumerGroupListView.getItems().clear();
        consumerGroupListView.getItems().addAll(activeConsumerGroups.keySet());
    }

    /**
     * 移除选中的消费者组
     */
    private void removeSelectedConsumerGroups() {
        java.util.List<String> selectedGroups = new java.util.ArrayList<>(consumerGroupListView.getSelectionModel().getSelectedItems());

        for (String groupId : selectedGroups) {
            // 停止消费者组
            ConsumerGroupManager manager = activeConsumerGroups.get(groupId);
            if (manager != null) {
                manager.stopAll();
                activeConsumerGroups.remove(groupId);
            }

            // 移除面板
            ConsumerGroupPanelController panel = consumerGroupPanels.get(groupId);
            if (panel != null) {
                consumerGroupsDisplayArea.getChildren().remove(panel.getPanel());
                consumerGroupPanels.remove(groupId);
            }

            log("已移除消费者组 '" + groupId + "'。");
        }

        // 更新列表
        updateConsumerGroupsList();
    }

    /**
     * 刷新消费者组列表
     */
    private void refreshConsumerGroupsList() {
        updateConsumerGroupsList();
        log("消费者组列表已刷新。");
    }

    /**
     * 更新选中的消费者组显示
     */
    private void updateSelectedConsumerGroupsDisplay() {
        // 这个方法可以根据需要实现，例如高亮显示选中的消费者组面板
    }

    /**
     * 清理资源
     */
    public void cleanup() {
        // 停止所有消费者组
        activeConsumerGroups.values().forEach(ConsumerGroupManager::stopAll);
        activeConsumerGroups.clear();

        // 移除所有消费者组面板
        consumerGroupsDisplayArea.getChildren().clear();
        consumerGroupPanels.clear();

        // 清空消费者组列表
        consumerGroupListView.getItems().clear();
    }

    /**
     * 设置所有控件的禁用状态
     * 
     * @param disable 是否禁用
     */
    public void setAllControlsDisable(boolean disable) {
        // 禁用主要控件
        consumerGroupIdField.setDisable(disable);
        topicCheckBoxContainer.setDisable(disable);
        autoCommitChoiceBox.setDisable(disable);
        consumerGroupListView.setDisable(disable);
        consumerGroupsDisplayArea.setDisable(disable);
        removeSelectedButton.setDisable(disable);
        refreshListButton.setDisable(disable);
    }
}
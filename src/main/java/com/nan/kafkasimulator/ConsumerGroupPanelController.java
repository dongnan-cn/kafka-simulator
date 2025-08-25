package com.nan.kafkasimulator;

import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.layout.VBox;

import org.apache.kafka.clients.admin.AdminClient;

import java.io.IOException;
import java.net.URL;
import java.util.ResourceBundle;

import static com.nan.kafkasimulator.utils.Logger.log;

/**
 * 消费者组面板控制器，负责管理单个消费者组的UI和交互
 */
public class ConsumerGroupPanelController implements Initializable {
    @FXML
    private VBox consumerGroupPanel;
    @FXML
    private Label consumerGroupIdLabel;
    @FXML
    private ToggleButton showPartitionAssignmentToggle;
    @FXML
    private VBox messageArea;
    @FXML
    private TextArea messageTextArea;
    @FXML
    private VBox partitionAssignmentArea;
    @FXML
    private TextArea partitionAssignmentTextArea;
    @FXML
    private Button addConsumerButton;
    @FXML
    private Button stopConsumerGroupButton;
    @FXML
    private Button resumeConsumerGroupButton;

    private ConsumerGroupManager consumerGroupManager;
    private AdminClient adminClient;

    public ConsumerGroupPanelController() {
    }

    /**
     * 创建一个新的消费者组面板
     * @param groupId 消费者组ID
     * @param topics 订阅的Topic列表
     * @param autoCommit 是否自动提交偏移量
     * @param bootstrapServers Kafka服务器地址
     * @param adminClient Kafka AdminClient
     * @return 消费者组面板控制器
     */
    public static ConsumerGroupPanelController create(String groupId, java.util.List<String> topics, 
            boolean autoCommit, String bootstrapServers, AdminClient adminClient) {
        try {
            FXMLLoader loader = new FXMLLoader(ConsumerGroupPanelController.class.getResource(
                    "/com/nan/kafkasimulator/fxml/consumer-group-panel.fxml"));
            VBox pane = loader.load();
            ConsumerGroupPanelController controller = loader.getController();

            // 初始化消费者组管理器
            controller.consumerGroupManager = new ConsumerGroupManager(
                    groupId, topics, autoCommit, bootstrapServers, 
                    controller.messageTextArea, controller.partitionAssignmentTextArea);

            // 设置消费者组ID
            controller.consumerGroupIdLabel.setText(groupId);

            // 设置AdminClient
            controller.adminClient = adminClient;

            // 启动消费者组
            controller.consumerGroupManager.start(1);

            return controller;
        } catch (IOException e) {
            log("创建消费者组面板失败: " + e.getMessage());
            return null;
        }
    }

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        // 绑定开关状态和分区分配区域的可见性
        partitionAssignmentArea.visibleProperty().bind(showPartitionAssignmentToggle.selectedProperty());

        // 初始状态为隐藏
        showPartitionAssignmentToggle.setSelected(false);

        // 绑定按钮事件
        stopConsumerGroupButton.setOnAction(event -> {
            consumerGroupManager.pauseAll();
            stopConsumerGroupButton.setDisable(true);
            resumeConsumerGroupButton.setDisable(false);
        });

        resumeConsumerGroupButton.setOnAction(event -> {
            consumerGroupManager.resumeAll();
            resumeConsumerGroupButton.setDisable(true);
            stopConsumerGroupButton.setDisable(false);
        });

        addConsumerButton.setOnAction(event -> consumerGroupManager.startNewConsumerInstance());

        showPartitionAssignmentToggle.setOnAction(event -> {
            if (showPartitionAssignmentToggle.isSelected() && adminClient != null) {
                consumerGroupManager.showPartitionAssignments(adminClient);
            }
        });
    }

    /**
     * 获取消费者组面板
     * @return 消费者组面板
     */
    public VBox getPanel() {
        return consumerGroupPanel;
    }

    /**
     * 获取消费者组ID
     * @return 消费者组ID
     */
    public String getGroupId() {
        return consumerGroupManager.getGroupId();
    }

    /**
     * 获取消费者组管理器
     * @return 消费者组管理器
     */
    public ConsumerGroupManager getConsumerGroupManager() {
        return consumerGroupManager;
    }

    /**
     * 停止消费者组
     */
    public void stop() {
        consumerGroupManager.stopAll();
    }

    /**
     * 设置AdminClient
     * @param adminClient Kafka AdminClient
     */
    public void setAdminClient(AdminClient adminClient) {
        this.adminClient = adminClient;
    }
}

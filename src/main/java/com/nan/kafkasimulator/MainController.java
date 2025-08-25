package com.nan.kafkasimulator;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import com.nan.kafkasimulator.manager.ConsumerGroupUIManager;
import com.nan.kafkasimulator.utils.Logger;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

public class MainController implements Initializable {

    @FXML
    private TopicManagementController topicManagementController;
    @FXML
    private ConnectionManagerController connectionManagementController;

    @FXML
    private ProducerController producerController;

    @FXML
    private TextArea logArea;
    @FXML
    private TextArea consumerMessagesArea; // 在FXML中动态添加，这里保留以便于在启动时引用
    @FXML
    private TextArea partitionAssignmentArea; // 在FXML中动态添加，这里保留以便于在启动时引用

    // 与消费者组管理Tab相关的UI元素
    @FXML
    private TabPane consumerTabPane;
    @FXML
    private ChoiceBox<String> autoCommitChoiceBox;

    // 管理器类
    // private KafkaConnectionManager connectionManager;
    // private MessageProducerManager messageProducerManager;
    private ConsumerGroupUIManager consumerGroupUIManager;

    // 用于管理所有消费者组实例的Map
    private final Map<String, ConsumerGroupManager> activeConsumerGroups = new HashMap<>();
    private final Map<String, Tab> consumerGroupTabs = new HashMap<>();

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        ControllerRegistry.setConnectionManagerController(connectionManagementController);
        ControllerRegistry.setTopicManagementController(topicManagementController);
        ControllerRegistry.setProducerController(producerController);

        setAllControlsDisable(true);

        // 初始状态，连接按钮可用，断开按钮不可用
        connectionManagementController.setStatusConnected(false);
        autoCommitChoiceBox.getItems().addAll("true", "false");
        autoCommitChoiceBox.setValue("true");
        Logger.getInstance().initialize(logArea);

        // 初始化管理器类
        connectionManagementController.setOnConnectionStateChanged(this::onConnectionStateChanged);
        // connectionManager = new KafkaConnectionManager(
        // bootstrapServersField.getText(),
        // this::onConnectionStateChanged);

        // 这些将在连接成功后初始化
        // messageProducerManager = null;
        consumerGroupUIManager = null;
    }

    /**
     * 连接状态变化时的回调函数
     * 
     * @param isConnected 是否已连接
     */
    private void onConnectionStateChanged(boolean isConnected) {
        javafx.application.Platform.runLater(() -> {
            if (isConnected) {
                // 连接成功后初始化其他管理器
                topicManagementController.setAdminClient(connectionManagementController.getAdminClient());
                topicManagementController.setOnTopicsUpdated(this::onTopicsUpdated);

                // messageProducerManager = new MessageProducerManager(
                // connectionManagementController.getProducer(),
                // producerTopicComboBox,
                // producerKeyField,
                // producerValueArea,
                // messagesPerSecondField,
                // dataTypeChoiceBox,
                // keyLengthField,
                // jsonFieldsCountField,
                // startAutoSendButton,
                // stopAutoSendButton,
                // onSendButtonClick,
                // sentCountLabel);

                consumerGroupUIManager = new ConsumerGroupUIManager(
                        consumerTabPane,
                        connectionManagementController.getBootstrapServers(),
                        activeConsumerGroups,
                        consumerGroupTabs);

                connectionManagementController.setConsumerGroupUIManager(consumerGroupUIManager);
                // 设置adminClient到ConsumerGroupUIManager
                consumerGroupUIManager.setAdminClient(connectionManagementController.getAdminClient());

                // 连接成功后自动刷新topic列表
                topicManagementController.refreshTopicsList();

                // 更新UI状态
                setAllControlsDisable(false);
                connectionManagementController.setStatusConnected(true);
                producerController.setStatusOnConnectionChanged(true);
            } else {
                // 连接失败或断开连接
                setAllControlsDisable(true);
                connectionManagementController.setStatusConnected(false);
                producerController.setStatusOnConnectionChanged(false);
            }
        });
    }

    /**
     * Topic列表更新时的回调函数
     * 
     * @param topicNames 更新后的Topic列表
     */
    private void onTopicsUpdated(java.util.List<String> topicNames) {
        if (consumerGroupUIManager != null) {
            consumerGroupUIManager.updateAllConsumerTopics(topicNames);
        }
    }

    private void setAllControlsDisable(boolean disable) {
        topicManagementController.setAllControlsDisable(disable);
        producerController.setControlsDisable(disable);

        // producerTopicComboBox.setDisable(disable);
        // producerKeyField.setDisable(disable);
        // producerValueArea.setDisable(disable);
        // acksChoiceBox.setDisable(disable);
        // batchSizeField.setDisable(disable);
        // lingerMsField.setDisable(disable);
        // onSendButtonClick.setDisable(disable);
        consumerTabPane.setDisable(disable);

        // messagesPerSecondField.setDisable(disable);

        // dataTypeChoiceBox.setDisable(disable);
        // sentCountLabel.setDisable(disable);
        // keyLengthField.setDisable(disable);
        // jsonFieldsCountField.setDisable(disable);
    }

    @FXML
    protected void onStartConsumerButtonClick() {
        if (consumerGroupUIManager != null) {
            consumerGroupUIManager.createConsumerGroup();
        }
    }

    // @FXML
    // protected void onProducerConfigChange() {
    // if (connectionManagementController != null &&
    // connectionManagementController.getProducer() != null) {
    // connectionManagementController.updateProducerConfig(
    // acksChoiceBox.getValue(),
    // batchSizeField.getText(),
    // lingerMsField.getText());
    // }
    // }

    public void cleanup() {
        Logger.log("正在关闭应用程序...");

        producerController.cleanup();

        if (consumerGroupUIManager != null) {
            consumerGroupUIManager.cleanup();
        }

        if (connectionManagementController != null) {
            connectionManagementController.disconnect();
        }

        Logger.log("所有资源已释放。");
    }
}

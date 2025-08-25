package com.nan.kafkasimulator;

import javafx.collections.FXCollections;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import com.nan.kafkasimulator.manager.MessageProducerManager;
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

    // @FXML
    // private ProducerController producerController;



    @FXML
    private TextArea logArea;

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
    private Button onSendButtonClick;
    @FXML
    private TextField messagesPerSecondField;
    @FXML
    private ChoiceBox<String> dataTypeChoiceBox;
    @FXML
    private Label sentCountLabel;
    @FXML
    private TextField keyLengthField;
    @FXML
    private TextField jsonFieldsCountField;
    @FXML
    private Button startAutoSendButton;
    @FXML
    private Button stopAutoSendButton;

    // 与消费者组管理Tab相关的UI元素
    @FXML
    private TabPane consumerTabPane;
    @FXML
    private ChoiceBox<String> autoCommitChoiceBox;

    // 管理器类
    // private KafkaConnectionManager connectionManager;
    private MessageProducerManager messageProducerManager;
    private ConsumerGroupUIManager consumerGroupUIManager;

    // 用于管理所有消费者组实例的Map
    private final Map<String, ConsumerGroupManager> activeConsumerGroups = new HashMap<>();
    private final Map<String, Tab> consumerGroupTabs = new HashMap<>();

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        ControllerRegistry.setConnectionManagerController(connectionManagementController);
        ControllerRegistry.setTopicManagementController(topicManagementController);

        acksChoiceBox.getItems().addAll("all", "1", "0");
        acksChoiceBox.setValue("1");

        setAllControlsDisable(true);
        
        // 初始状态，连接按钮可用，断开按钮不可用
        connectionManagementController.setStatusConnected(false);
        startAutoSendButton.setDisable(true);
        stopAutoSendButton.setDisable(true);
        autoCommitChoiceBox.getItems().addAll("true", "false");
        autoCommitChoiceBox.setValue("true");
        dataTypeChoiceBox.setItems(FXCollections.observableArrayList("String", "JSON"));
        dataTypeChoiceBox.setValue("String");
        dataTypeChoiceBox.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            jsonFieldsCountField.setDisable(!"JSON".equals(newVal));
            if ("JSON".equals(newVal)) {
                producerValueArea.setDisable(true);
                producerValueArea.setPromptText("JSON数据将自动生成");
            } else {
                producerValueArea.setDisable(false);
                producerValueArea.setPromptText("输入要发送的消息");
            }
        });
        Logger.getInstance().initialize(logArea);

        // 初始化管理器类
        connectionManagementController.setOnConnectionStateChanged(this::onConnectionStateChanged);
        // connectionManager = new KafkaConnectionManager(
        // bootstrapServersField.getText(),
        // this::onConnectionStateChanged);

        // 这些将在连接成功后初始化
        messageProducerManager = null;
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
                topicManagementController.setProducerTopicComboBox(producerTopicComboBox);
                topicManagementController.setOnTopicsUpdated(this::onTopicsUpdated);

                messageProducerManager = new MessageProducerManager(
                        connectionManagementController.getProducer(),
                        producerTopicComboBox,
                        producerKeyField,
                        producerValueArea,
                        messagesPerSecondField,
                        dataTypeChoiceBox,
                        keyLengthField,
                        jsonFieldsCountField,
                        startAutoSendButton,
                        stopAutoSendButton,
                        onSendButtonClick,
                        sentCountLabel);

                consumerGroupUIManager = new ConsumerGroupUIManager(
                        consumerTabPane,
                        connectionManagementController.getBootstrapServers(),
                        activeConsumerGroups,
                        consumerGroupTabs);

                connectionManagementController.setMessageProducerManager(messageProducerManager);
                connectionManagementController.setConsumerGroupUIManager(consumerGroupUIManager);
                // 设置adminClient到ConsumerGroupUIManager
                consumerGroupUIManager.setAdminClient(connectionManagementController.getAdminClient());

                // 连接成功后自动刷新topic列表
                topicManagementController.refreshTopicsList();

                // 更新UI状态
                setAllControlsDisable(false);
                connectionManagementController.setStatusConnected(true);
                startAutoSendButton.setDisable(false);
            } else {
                // 连接失败或断开连接
                setAllControlsDisable(true);
                connectionManagementController.setStatusConnected(false);
                startAutoSendButton.setDisable(true);
                stopAutoSendButton.setDisable(true);
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
        producerTopicComboBox.setDisable(disable);
        producerKeyField.setDisable(disable);
        producerValueArea.setDisable(disable);
        acksChoiceBox.setDisable(disable);
        batchSizeField.setDisable(disable);
        lingerMsField.setDisable(disable);
        onSendButtonClick.setDisable(disable);
        consumerTabPane.setDisable(disable);

        messagesPerSecondField.setDisable(disable);

        dataTypeChoiceBox.setDisable(disable);
        sentCountLabel.setDisable(disable);
        keyLengthField.setDisable(disable);
        jsonFieldsCountField.setDisable(disable);
    }

    @FXML
    protected void onSendButtonClick() {
        if (messageProducerManager != null) {
            messageProducerManager.sendMessage();
        }
    }

    @FXML
    private void onStartAutoSendButtonClick() {
        if (messageProducerManager != null) {
            messageProducerManager.startAutoSend();
        }
    }

    @FXML
    private void onStopAutoSendButtonClick() {
        if (messageProducerManager != null) {
            messageProducerManager.stopAutoSend();
        }
    }

    @FXML
    protected void onStartConsumerButtonClick() {
        if (consumerGroupUIManager != null) {
            consumerGroupUIManager.createConsumerGroup();
        }
    }

    @FXML
    protected void onProducerConfigChange() {
        if (connectionManagementController != null && connectionManagementController.getProducer() != null) {
            connectionManagementController.updateProducerConfig(
                    acksChoiceBox.getValue(),
                    batchSizeField.getText(),
                    lingerMsField.getText());
        }
    }

    public void cleanup() {
        Logger.log("正在关闭应用程序...");

        if (messageProducerManager != null) {
            messageProducerManager.cleanup();
        }

        if (consumerGroupUIManager != null) {
            consumerGroupUIManager.cleanup();
        }

        if (connectionManagementController != null) {
            connectionManagementController.disconnect();
        }

        Logger.log("所有资源已释放。");
    }
}

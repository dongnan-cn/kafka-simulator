package com.nan.kafkasimulator;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import com.nan.kafkasimulator.utils.Logger;

import java.net.URL;
import java.util.ResourceBundle;

public class MainController implements Initializable {

    @FXML
    private TopicManagementController topicManagementController;
    @FXML
    private ConnectionManagerController connectionManagementController;

    @FXML
    private ProducerController producerController;
    @FXML
    private ConsumerController consumerController;

    @FXML
    private LogController logManagementController;

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        ControllerRegistry.setConnectionManagerController(connectionManagementController);
        ControllerRegistry.setTopicManagementController(topicManagementController);
        ControllerRegistry.setProducerController(producerController);
        ControllerRegistry.setConsumerController(consumerController);

        setAllControlsDisable(true);

        // 初始状态，连接按钮可用，断开按钮不可用
        connectionManagementController.setStatusConnected(false);

        // 初始化管理器类
        connectionManagementController.setOnConnectionStateChanged(this::onConnectionStateChanged);
    }

    /**
     * 连接状态变化时的回调函数
     * 
     * @param isConnected 是否已连接
     */
    private void onConnectionStateChanged(boolean isConnected) {
        javafx.application.Platform.runLater(() -> {
            if (isConnected) {
                topicManagementController.setAdminClient(connectionManagementController.getAdminClient());
                topicManagementController.setOnTopicsUpdated(this::onTopicsUpdated);


                consumerController.setBootstrapServers(connectionManagementController.getBootstrapServers());
                consumerController.setAdminClient(connectionManagementController.getAdminClient());
                topicManagementController.refreshTopicsList();

                setAllControlsDisable(false);
                connectionManagementController.setStatusConnected(true);
                producerController.setStatusOnConnectionChanged(true);
            } else {
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
        consumerController.updateAllConsumerTopics(topicNames);
        producerController.updateTabs(topicNames);
    }

    private void setAllControlsDisable(boolean disable) {
        topicManagementController.setAllControlsDisable(disable);
        producerController.setAllControlsDisable(disable);
        consumerController.setControlsDisable(disable);
    }

    public void cleanup() {
        Logger.log("正在关闭应用程序...");

        producerController.cleanup();
        if (consumerController != null) {
            consumerController.cleanup();
        }

        if (connectionManagementController != null) {
            connectionManagementController.disconnect();
        }

        Logger.log("所有资源已释放。");
    }
}

package com.nan.kafkasimulator;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.TabPane;

import com.nan.kafkasimulator.controller.ConnectionManagerController;
import com.nan.kafkasimulator.controller.ConsumerController;
import com.nan.kafkasimulator.controller.LogController;
import com.nan.kafkasimulator.controller.ProducerController;
import com.nan.kafkasimulator.controller.TopicManagementController;
import com.nan.kafkasimulator.controller.BrokerFailureController;
import com.nan.kafkasimulator.monitoring.ui.MonitoringDashboardController;
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
    
    @FXML
    private TabPane mainTabPane;
    
    @FXML
    private BrokerFailureController brokerFailureController;
    
    @FXML
    private MonitoringDashboardController monitoringDashboardController;

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        ControllerRegistry.setConnectionManagerController(connectionManagementController);
        ControllerRegistry.setTopicManagementController(topicManagementController);
        ControllerRegistry.setProducerController(producerController);
        ControllerRegistry.setConsumerController(consumerController);
        ControllerRegistry.setBrokerFailureController(brokerFailureController);
        ControllerRegistry.setMonitoringDashboardController(monitoringDashboardController);

        setAllControlsDisable(true);

        // Initial state, connect button available, disconnect button unavailable
        connectionManagementController.setStatusConnected(false);

        // Initialize manager classes
        connectionManagementController.setOnConnectionStateChanged(this::onConnectionStateChanged);
    }

    /**
     * Callback function when connection state changes
     * 
     * @param isConnected Whether connected
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
                
                // Set connection status for Broker failure controller
                if (brokerFailureController != null) {
                    brokerFailureController.setConnected(true);
                }
                
                // Set connection status for Monitoring dashboard controller
                if (monitoringDashboardController != null) {
                    monitoringDashboardController.setConnected(true);
                }
            } else {
                setAllControlsDisable(true);
                connectionManagementController.setStatusConnected(false);
                producerController.setStatusOnConnectionChanged(false);
                
                // Set connection status for Broker failure controller
                if (brokerFailureController != null) {
                    brokerFailureController.setConnected(false);
                }
                
                // Set connection status for Monitoring dashboard controller
                if (monitoringDashboardController != null) {
                    monitoringDashboardController.setConnected(false);
                }
            }
        });
    }

    /**
     * Callback function when Topic list is updated
     * 
     * @param topicNames Updated Topic list
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
        Logger.log("Shutting down application...");

        producerController.cleanup();
        if (consumerController != null) {
            consumerController.cleanup();
        }

        if (connectionManagementController != null) {
            connectionManagementController.disconnect();
        }
        
        // Clean up Broker failure controller
        if (brokerFailureController != null) {
            brokerFailureController.cleanup();
        }
        
        // Clean up Monitoring dashboard controller
        if (monitoringDashboardController != null) {
            monitoringDashboardController.cleanup();
        }

        Logger.log("All resources have been released.");
    }
}

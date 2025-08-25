package com.nan.kafkasimulator;

import javafx.fxml.FXML;

public class ControllerRegistry {

    private static TopicManagementController topicManagementController;

    private static ConnectionManagerController connectionManagerController;

    public static TopicManagementController getTopicManagementController() {
        return topicManagementController;
    }

    public static void setTopicManagementController(TopicManagementController topicManagementController) {
        ControllerRegistry.topicManagementController = topicManagementController;
    }

    public static ConnectionManagerController getConnectionManagerController() {
        return connectionManagerController;
    }

    public static void setConnectionManagerController(ConnectionManagerController connectionManagerController) {
        ControllerRegistry.connectionManagerController = connectionManagerController;
    }

}

package com.nan.kafkasimulator;

public class ControllerRegistry {

    private static TopicManagementController topicManagementController;

    private static ConnectionManagerController connectionManagerController;
    private static ProducerController producerController;

    public static ProducerController getProducerController() {
        return producerController;
    }

    public static void setProducerController(ProducerController producerController) {
        ControllerRegistry.producerController = producerController;
    }

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

package com.nan.kafkasimulator;

import com.nan.kafkasimulator.controller.ConnectionManagerController;
import com.nan.kafkasimulator.controller.ConsumerController;
import com.nan.kafkasimulator.controller.ProducerController;
import com.nan.kafkasimulator.controller.TopicManagementController;
import com.nan.kafkasimulator.controller.BrokerFailureController;

public class ControllerRegistry {

    private static TopicManagementController topicManagementController;

    private static ConnectionManagerController connectionManagerController;
    private static ProducerController producerController;
    private static ConsumerController consumerController;
    private static BrokerFailureController brokerFailureController;

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

    public static ConsumerController getConsumerController() {
        return consumerController;
    }

    public static void setConsumerController(ConsumerController consumerController) {
        ControllerRegistry.consumerController = consumerController;
    }

    public static BrokerFailureController getBrokerFailureController() {
        return brokerFailureController;
    }

    public static void setBrokerFailureController(BrokerFailureController brokerFailureController) {
        ControllerRegistry.brokerFailureController = brokerFailureController;
    }
}

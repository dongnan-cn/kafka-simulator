package com.nan.kafkasimulator.controller;

import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.control.*;

import static com.nan.kafkasimulator.utils.Logger.log;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProducerController {

    @FXML
    private TabPane producerTabPane;
    @FXML
    private Tab defaultTab;

    private final Map<String, ProducerTabController> topicControllers = new HashMap<>();

    // 更新Tab，根据当前存在的topic创建或删除tab
    public void updateTabs(List<String> topicNames) {
        // 移除不存在的topic对应的tab
        topicControllers.keySet().removeIf(topic -> {
            if (!topicNames.contains(topic)) {
                // 找到并移除对应的tab
                Tab tabToRemove = null;
                for (Tab tab : producerTabPane.getTabs()) {
                    if (tab.getId() != null && tab.getId().equals("tab-" + topic)) {
                        tabToRemove = tab;
                        break;
                    }
                }
                if (tabToRemove != null) {
                    producerTabPane.getTabs().remove(tabToRemove);
                    // 清理资源
                    ProducerTabController controller = topicControllers.get(topic);
                    if (controller != null) {
                        controller.cleanup();
                    }
                }
                return true;
            }
            return false;
        });

        // 添加新topic对应的tab
        for (String topic : topicNames) {
            if (!topicControllers.containsKey(topic)) {
                try {
                    // 创建新的tab
                    Tab newTab = new Tab(topic);
                    newTab.setId("tab-" + topic);

                    // 加载tab内容
                    FXMLLoader loader = new FXMLLoader(
                            getClass().getResource("/com/nan/kafkasimulator/fxml/producer-tab-content.fxml"));
                    // 设置控制器
                    ProducerTabController controller = new ProducerTabController(topic);
                    loader.setController(controller);

                    // 加载FXML内容
                    newTab.setContent(loader.load());

                    // 添加到tab pane
                    producerTabPane.getTabs().add(newTab);

                    // 保存控制器引用
                    topicControllers.put(topic, controller);
                    log("Created producer configuration Tab for Topic [" + topic + "]");
                } catch (Exception e) {
                    log("Failed to create producer configuration Tab for Topic [" + topic + "]: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }

        resetTabs();
    }

    private void resetTabs() {
        // If no tabs exist, show default tab
        if (topicControllers.isEmpty()) {
            producerTabPane.getTabs().setAll(defaultTab);
        } else if (producerTabPane.getTabs().contains(defaultTab)) {
            // Remove default tab
            producerTabPane.getTabs().remove(defaultTab);
        }
    }

    // Get producer controller for specified topic
    public ProducerTabController getTopicController(String topic) {
        return topicControllers.get(topic);
    }

    // Set disable status for all producer controls
    public void setAllControlsDisable(boolean disable) {
        for (ProducerTabController controller : topicControllers.values()) {
            controller.setControlsDisable(disable);
        }
    }

    // Set handling for connection status changes
    public void setStatusOnConnectionChanged(boolean connected) {
        for (ProducerTabController controller : topicControllers.values()) {
            controller.setStatusOnConnectionChanged(connected);
        }
    }

    // Close all producers
    public void closeAllProducers() {
        for (ProducerTabController controller : topicControllers.values()) {
            controller.closeProducer();
        }
    }

    // Clean up all resources
    public void cleanup() {
        for (ProducerTabController controller : topicControllers.values()) {
            controller.cleanup();
        }
        log("topicControllers cleared");
        topicControllers.clear();
        resetTabs();
    }
}
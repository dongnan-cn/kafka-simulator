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
                    log("已创建Topic [" + topic + "] 的生产者配置Tab");
                } catch (Exception e) {
                    log("创建Topic [" + topic + "] 的生产者配置Tab失败: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }

        resetTabs();
    }

    private void resetTabs() {
        // 如果没有tab，显示默认tab
        if (topicControllers.isEmpty()) {
            producerTabPane.getTabs().setAll(defaultTab);
        } else if (producerTabPane.getTabs().contains(defaultTab)) {
            // 移除默认tab
            producerTabPane.getTabs().remove(defaultTab);
        }
    }

    // 获取指定topic的生产者控制器
    public ProducerTabController getTopicController(String topic) {
        return topicControllers.get(topic);
    }

    // 设置所有生产者控件的禁用状态
    public void setAllControlsDisable(boolean disable) {
        for (ProducerTabController controller : topicControllers.values()) {
            controller.setControlsDisable(disable);
        }
    }

    // 设置连接状态变化时的处理
    public void setStatusOnConnectionChanged(boolean connected) {
        for (ProducerTabController controller : topicControllers.values()) {
            controller.setStatusOnConnectionChanged(connected);
        }
    }

    // 关闭所有生产者
    public void closeAllProducers() {
        for (ProducerTabController controller : topicControllers.values()) {
            controller.closeProducer();
        }
    }

    // 清理所有资源
    public void cleanup() {
        for (ProducerTabController controller : topicControllers.values()) {
            controller.cleanup();
        }
        log("topicControllers cleared");
        topicControllers.clear();
        resetTabs();
    }
}
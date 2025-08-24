package com.nan.kafkasimulator.manager;

import javafx.application.Platform;
import javafx.scene.control.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static com.nan.kafkasimulator.utils.Logger.log;

/**
 * 负责管理Kafka Topic的类，包括创建、删除和刷新Topic列表等操作
 */
public class TopicManager {
    private final AdminClient adminClient;
    private final ListView<String> topicsListView;
    private final ComboBox<String> producerTopicComboBox;
    private final Consumer<List<String>> onTopicsUpdated;

    public TopicManager(AdminClient adminClient, ListView<String> topicsListView, 
                       ComboBox<String> producerTopicComboBox, Consumer<List<String>> onTopicsUpdated) {
        this.adminClient = adminClient;
        this.topicsListView = topicsListView;
        this.producerTopicComboBox = producerTopicComboBox;
        this.onTopicsUpdated = onTopicsUpdated;
    }

    public void refreshTopicsList() {
        if (adminClient == null) {
            log("错误: 请先连接到 Kafka 集群。");
            return;
        }

        log("正在刷新 Topic 列表...");
        try {
            Set<String> topicNames = adminClient.listTopics().names().get();

            Platform.runLater(() -> {
                topicsListView.getItems().clear();
                topicsListView.getItems().addAll(topicNames);
                producerTopicComboBox.getItems().clear();
                producerTopicComboBox.getItems().addAll(topicNames);
                onTopicsUpdated.accept(new ArrayList<>(topicNames));
                log("Topic 列表刷新成功。");
            });
        } catch (ExecutionException | InterruptedException e) {
            log("刷新 Topic 列表失败: " + e.getMessage());
        }
    }

    public void createTopic(String topicName, int numPartitions, short replicationFactor) {
        if (adminClient == null) {
            log("错误: 请先连接到 Kafka 集群。");
            return;
        }

        if (topicName == null || topicName.trim().isEmpty()) {
            log("错误: Topic 名称不能为空。");
            return;
        }

        try {
            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            log("成功创建 Topic: " + topicName);
            refreshTopicsList();
        } catch (NumberFormatException e) {
            log("错误: 分区数和副本因子必须是有效的数字。");
            showAlert("输入错误", null, "分区数和副本因子必须是有效的数字。");
        } catch (ExecutionException | InterruptedException e) {
            log("创建 Topic 失败: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
        }
    }

    public void deleteTopic() {
        if (adminClient == null) {
            log("错误: 请先连接到 Kafka 集群。");
            return;
        }

        String selectedTopic = topicsListView.getSelectionModel().getSelectedItem();
        if (selectedTopic == null || selectedTopic.trim().isEmpty()) {
            log("错误: 请选择一个 Topic 来删除。");
            return;
        }

        try {
            adminClient.deleteTopics(Collections.singleton(selectedTopic)).all().get();
            log("成功删除 Topic: " + selectedTopic);
            refreshTopicsList();
        } catch (ExecutionException | InterruptedException e) {
            log("删除 Topic 失败: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
        }
    }

    private void showAlert(String title, String header, String content) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle(title);
        alert.setHeaderText(header);
        alert.setContentText(content);
        alert.showAndWait();
    }
}

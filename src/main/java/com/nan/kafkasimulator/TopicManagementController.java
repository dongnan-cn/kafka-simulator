package com.nan.kafkasimulator;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.ListView;
import javafx.scene.control.TextField;

import static com.nan.kafkasimulator.utils.Logger.log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import com.nan.kafkasimulator.utils.Logger;

// 注意：这里不再实现 Initializable 接口
public class TopicManagementController {

    // FXML 变量，与 topic-management.fxml 中的 fx:id 对应
    @FXML
    private ListView<String> topicsListView;

    public ListView<String> getTopicsListView() {
        return topicsListView;
    }

    @FXML
    private Button onRefreshTopicsButtonClick;
    @FXML
    private TextField newTopicNameField;
    @FXML
    private TextField numPartitionsField;
    @FXML
    private TextField replicationFactorField;
    @FXML
    private Button onCreateTopicButtonClick;
    @FXML
    private Button onDeleteTopicButtonClick;

    private AdminClient adminClient;
    private Consumer<List<String>> onTopicsUpdated;

    void setAllControlsDisable(boolean disable) {
        newTopicNameField.setDisable(disable);
        numPartitionsField.setDisable(disable);
        replicationFactorField.setDisable(disable);
        onCreateTopicButtonClick.setDisable(disable);
        onRefreshTopicsButtonClick.setDisable(disable);
        onDeleteTopicButtonClick.setDisable(disable);
        topicsListView.setDisable(disable);
    }

    public void setAdminClient(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    public void setOnTopicsUpdated(Consumer<List<String>> onTopicsUpdated) {
        this.onTopicsUpdated = onTopicsUpdated;
    }

    // FXML 文件的 onAction 绑定
    @FXML
    protected void onRefreshTopicsButtonClick() {
        refreshTopicsList();

    }

    @FXML
    protected void onCreateTopicButtonClick() {
        try {
            int numPartitions = Integer.parseInt(numPartitionsField.getText());
            short replicationFactor = Short.parseShort(replicationFactorField.getText());
            createTopic(newTopicNameField.getText(), numPartitions, replicationFactor);
        } catch (NumberFormatException e) {
            Logger.log("错误: 分区数和副本因子必须是有效的数字。");
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

    private void showAlert(String title, String header, String content) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle(title);
        alert.setHeaderText(header);
        alert.setContentText(content);
        alert.showAndWait();
    }

    @FXML
    protected void onDeleteTopicButtonClick() {
        deleteTopic();
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
                ComboBox<String> comboBoxTopic = ControllerRegistry.getProducerController().getProducerTopicComboBox();
                topicsListView.getItems().clear();
                topicsListView.getItems().addAll(topicNames);
                comboBoxTopic.getItems().clear();
                comboBoxTopic.getItems().addAll(topicNames);
                onTopicsUpdated.accept(new ArrayList<>(topicNames));
                log("Topic 列表刷新成功。");
            });
        } catch (ExecutionException | InterruptedException e) {
            log("刷新 Topic 列表失败: " + e.getMessage());
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

}

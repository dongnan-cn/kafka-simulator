package com.nan.kafkasimulator;

import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.ListView;
import javafx.scene.control.TextField;

import com.nan.kafkasimulator.manager.TopicManager;
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
    private Button refreshTopicsButton;
    @FXML
    private TextField newTopicNameField;
    @FXML
    private TextField numPartitionsField;
    @FXML
    private TextField replicationFactorField;
    @FXML
    private Button createTopicButton;
    @FXML
    private Button deleteTopicButton;

    private TopicManager topicManager;

    // FXML 文件的 onAction 绑定
    @FXML
    protected void onRefreshTopicsButtonClick() {
        if (topicManager != null) {
            topicManager.refreshTopicsList();
        }
    }

    @FXML
    protected void onCreateTopicButtonClick() {
        if (topicManager != null) {
            try {
                int numPartitions = Integer.parseInt(numPartitionsField.getText());
                short replicationFactor = Short.parseShort(replicationFactorField.getText());
                topicManager.createTopic(newTopicNameField.getText(), numPartitions, replicationFactor);
            } catch (NumberFormatException e) {
                Logger.log("错误: 分区数和副本因子必须是有效的数字。");
            }
        }
    }

    @FXML
    protected void onDeleteTopicButtonClick() {
        if (topicManager != null) {
            topicManager.deleteTopic();
        }
    }

    // 设置 TopicManager 实例的方法
    public void setTopicManager(TopicManager topicManager) {
        this.topicManager = topicManager;
        // 在此处或其他地方可以调用初始化逻辑，例如刷新主题列表
    }
}

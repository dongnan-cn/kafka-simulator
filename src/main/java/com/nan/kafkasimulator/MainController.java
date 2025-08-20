package com.nan.kafkasimulator;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Node;

import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class MainController implements Initializable {

    @FXML
    private TextField bootstrapServersField;
    @FXML
    private Button connectButton;
    @FXML
    private TextArea logArea;
    @FXML
    private TextField newTopicNameField;
    @FXML
    private TextField numPartitionsField;
    @FXML
    private TextField replicationFactorField;
    @FXML
    private ListView<String> topicsListView;

    private AdminClient adminClient;

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        bootstrapServersField.setText("localhost:19092");
    }

    @FXML
    protected void onConnectButtonClick() {
        String bootstrapServers = bootstrapServersField.getText();
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            appendToLog("错误: 请输入 Kafka 集群地址。");
            return;
        }

        appendToLog("正在尝试连接到 Kafka 集群: " + bootstrapServers);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try {
            if (adminClient != null) {
                adminClient.close();
            }
            adminClient = AdminClient.create(props);

            // 验证连接并获取元数据
            displayClusterMetadata();
            appendToLog("成功连接到 Kafka 集群！");

            Thread.sleep(1000);
            // 刷新 Topic 列表
            refreshTopicsList();
        } catch (Exception e) {
            appendToLog("连接失败: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
        }
    }

    private void displayClusterMetadata() throws ExecutionException, InterruptedException {
        if (adminClient == null)
            return;
        DescribeClusterResult describeClusterResult = adminClient.describeCluster();
        Collection<Node> nodes = describeClusterResult.nodes().get();

        appendToLog("\n--- Broker 信息 ---");
        for (Node node : nodes) {
            appendToLog("Broker ID: " + node.id() + ", 地址: " + node.host() + ":" + node.port());
        }
    }

    @FXML
    protected void onRefreshTopicsButtonClick() {
        refreshTopicsList();
    }

    private void refreshTopicsList() {
        if (adminClient == null) {
            appendToLog("错误: 请先连接到 Kafka 集群。");
            return;
        }

        appendToLog("正在刷新 Topic 列表...");
        try {
            System.out.println("DEBUG: 正在调用 listTopics()。");
            Set<String> topicNames = adminClient.listTopics().names().get();
            System.out.println("DEBUG: 从 Kafka 获取到的 Topic 列表数量：" + topicNames.size());
            topicNames.forEach(name -> System.out.println("DEBUG: 获取到 Topic: " + name));

            Platform.runLater(() -> {
                topicsListView.getItems().clear();
                topicsListView.getItems().addAll(topicNames);

                appendToLog("Topic 列表刷新成功。");
                System.out.println("DEBUG: ListView 已更新。");
            });
        } catch (ExecutionException | InterruptedException e) {
            appendToLog("刷新 Topic 列表失败: " + e.getMessage());
            e.printStackTrace(); // 打印完整的堆栈信息
        }
    }

    @FXML
    protected void onSendButtonClick() {
        // TODO: 在这里添加消息发送逻辑
        appendToLog("发送消息按钮被点击，功能待实现。");
    }

    @FXML
    protected void onStartConsumerButtonClick() {
        // TODO: 在这里添加启动消费者的逻辑
        appendToLog("启动消费者按钮被点击，功能待实现。");
    }

    @FXML
    protected void onCreateTopicButtonClick() {
        if (adminClient == null) {
            appendToLog("错误: 请先连接到 Kafka 集群。");
            return;
        }

        String topicName = newTopicNameField.getText();
        if (topicName == null || topicName.trim().isEmpty()) {
            appendToLog("错误: Topic 名称不能为空。");
            return;
        }

        int numPartitions = Integer.parseInt(numPartitionsField.getText());
        short replicationFactor = Short.parseShort(replicationFactorField.getText());

        NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);

        try {
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            appendToLog("成功创建 Topic: " + topicName);
            // 立即刷新列表以显示新创建的 Topic
            refreshTopicsList();
        } catch (ExecutionException | InterruptedException e) {
            appendToLog("创建 Topic 失败: " + e.getCause().getMessage());
        }
    }

    @FXML
    protected void onDeleteTopicButtonClick() {
        if (adminClient == null) {
            appendToLog("错误: 请先连接到 Kafka 集群。");
            return;
        }

        String selectedTopic = topicsListView.getSelectionModel().getSelectedItem();
        if (selectedTopic == null || selectedTopic.trim().isEmpty()) {
            appendToLog("错误: 请选择一个 Topic 来删除。");
            return;
        }

        try {
            adminClient.deleteTopics(Collections.singleton(selectedTopic)).all().get();
            appendToLog("成功删除 Topic: " + selectedTopic);
            // 立即刷新列表以移除已删除的 Topic
            refreshTopicsList();
        } catch (ExecutionException | InterruptedException e) {
            appendToLog("删除 Topic 失败: " + e.getCause().getMessage());
        }
    }

    private void appendToLog(String message) {
        Platform.runLater(() -> {
            logArea.appendText(message + "\n");
        });
    }
}
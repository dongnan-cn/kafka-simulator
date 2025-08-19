package com.nan.kafkasimulator;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class MainController {

    @FXML
    private TextField bootstrapServersField;

    @FXML
    private Button connectButton;

    @FXML
    private TextArea logArea;

    @FXML
    protected void onConnectButtonClick() {
        String bootstrapServers = bootstrapServersField.getText();
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            appendToLog("错误: 请输入 Kafka 集群地址。");
            return;
        }

        appendToLog("正在尝试连接到 Kafka 集群: " + bootstrapServers);

        // 创建 Kafka AdminClient 实例来测试连接
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-simulator-test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        try (Admin adminClient = AdminClient.create(props)) {
            displayClusterMetadata(adminClient);
            appendToLog("成功连接到 Kafka 集群！");
        } catch (ExecutionException | InterruptedException e) {
            appendToLog("连接失败: " + e.getCause().getMessage());
        } catch (Exception e) {
            appendToLog("连接失败: " + e.getMessage());
        }
    }

    private void displayClusterMetadata(Admin adminClient) throws ExecutionException, InterruptedException {
        // 获取并打印 Broker 信息
        DescribeClusterResult describeClusterResult = adminClient.describeCluster();
        Collection<org.apache.kafka.common.Node> nodes = describeClusterResult.nodes().get();
        appendToLog("\n--- Broker 信息 ---");
        for (org.apache.kafka.common.Node node : nodes) {
            appendToLog("Broker ID: " + node.id() + ", 地址: " + node.host() + ":" + node.port());
        }

        // 获取并打印所有 Topic 的信息
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> topicNames = listTopicsResult.names().get();

        appendToLog("\n--- Topic 和分区信息 ---");
        if (topicNames.isEmpty()) {
            appendToLog("集群中没有 Topic。");
            return;
        }

        Map<String, TopicDescription> topicDescriptions = adminClient.describeTopics(topicNames).allTopicNames().get();
        for (Map.Entry<String, TopicDescription> entry : topicDescriptions.entrySet()) {
            TopicDescription topicDescription = entry.getValue();
            appendToLog("Topic: " + topicDescription.name());
            topicDescription.partitions().forEach(partition -> {
                appendToLog("  - 分区 " + partition.partition() + ":");
                appendToLog("    - Leader: " + partition.leader().id() + " (Broker " + partition.leader().host() + ")");
                appendToLog("    - 副本 (Replicas): " + partition.replicas().stream()
                        .map(node -> String.valueOf(node.id())).reduce((a, b) -> a + ", " + b).orElse("无"));
                appendToLog("    - 同步副本 (ISRs): " + partition.isr().stream().map(node -> String.valueOf(node.id()))
                        .reduce((a, b) -> a + ", " + b).orElse("无"));
            });
        }
    }

    private void appendToLog(String message) {
        Platform.runLater(() -> logArea.appendText(message + "\n"));
    }
}
package com.nan.kafkasimulator;

import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
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
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            listTopicsResult.names().get(); // 阻塞等待结果，验证连接
            appendToLog("成功连接到 Kafka 集群！");
        } catch (ExecutionException | InterruptedException e) {
            appendToLog("连接失败: " + e.getCause().getMessage());
        } catch (Exception e) {
            appendToLog("连接失败: " + e.getMessage());
        }
    }

    private void appendToLog(String message) {
        logArea.appendText(message + "\n");
    }
}
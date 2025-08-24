package com.nan.kafkasimulator.manager;

import javafx.application.Platform;
import javafx.scene.control.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.nan.kafkasimulator.utils.Logger.log;

/**
 * 负责管理Kafka消息生产的类，包括发送单条消息和自动发送消息等操作
 */
public class MessageProducerManager {
    private final KafkaProducer<String, String> producer;
    private final ComboBox<String> producerTopicComboBox;
    private final TextField producerKeyField;
    private final TextArea producerValueArea;
    private final TextField messagesPerSecondField;
    private final ChoiceBox<String> dataTypeChoiceBox;
    private final TextField keyLengthField;
    private final TextField jsonFieldsCountField;
    private final Button startAutoSendButton;
    private final Button stopAutoSendButton;
    private final Button sendButton;
    private final Label sentCountLabel;

    private final Map<String, ScheduledExecutorService> autoSendExecutors = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> sentCounts = new ConcurrentHashMap<>();
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private final Random random = new Random();

    public MessageProducerManager(KafkaProducer<String, String> producer, 
                                ComboBox<String> producerTopicComboBox,
                                TextField producerKeyField, 
                                TextArea producerValueArea,
                                TextField messagesPerSecondField,
                                ChoiceBox<String> dataTypeChoiceBox,
                                TextField keyLengthField,
                                TextField jsonFieldsCountField,
                                Button startAutoSendButton,
                                Button stopAutoSendButton,
                                Button sendButton,
                                Label sentCountLabel) {
        this.producer = producer;
        this.producerTopicComboBox = producerTopicComboBox;
        this.producerKeyField = producerKeyField;
        this.producerValueArea = producerValueArea;
        this.messagesPerSecondField = messagesPerSecondField;
        this.dataTypeChoiceBox = dataTypeChoiceBox;
        this.keyLengthField = keyLengthField;
        this.jsonFieldsCountField = jsonFieldsCountField;
        this.startAutoSendButton = startAutoSendButton;
        this.stopAutoSendButton = stopAutoSendButton;
        this.sendButton = sendButton;
        this.sentCountLabel = sentCountLabel;
    }

    public void sendMessage() {
        if (producer == null) {
            log("错误: 请先连接到 Kafka 集群。");
            return;
        }

        String topicName = producerTopicComboBox.getValue();
        if (topicName == null || topicName.isEmpty()) {
            log("错误: 请选择一个 Topic。");
            return;
        }

        String key = producerKeyField.getText();
        String value = producerValueArea.getText();

        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

            log("正在发送消息...");
            producer.send(record, (metadata, exception) -> {
                Platform.runLater(() -> {
                    if (exception == null) {
                        log("消息发送成功！");
                        log("  - Topic: " + metadata.topic());
                        log("  - 分区: " + metadata.partition());
                        log("  - 偏移量: " + metadata.offset());
                    } else {
                        log("消息发送失败: " + exception.getMessage());
                    }
                });
            });
        } catch (Exception e) {
            log("消息发送失败: " + e.getMessage());
        }
    }

    public void startAutoSend() {
        String topic = producerTopicComboBox.getValue();
        if (producer == null || topic == null || topic.isEmpty()) {
            log("错误: 请先连接到 Kafka 并选择一个 Topic。");
            if (producer == null) {
                log("错误: producer is null");
            } else {
                log("错误: topic is " + topic);
            }
            return;
        }

        if (autoSendExecutors.containsKey(topic)) {
            log("错误: Topic \"" + topic + "\" 的自动发送任务已在运行。");
            return;
        }

        try {
            double messagesPerSecond = Double.parseDouble(messagesPerSecondField.getText());
            if (messagesPerSecond <= 0) {
                log("错误: 每秒消息数必须大于 0。");
                return;
            }
            long intervalMs = (long) (1000 / messagesPerSecond);
            String dataType = dataTypeChoiceBox.getValue();
            int keyLength = Integer.parseInt(keyLengthField.getText());
            int jsonFieldsCount = Integer.parseInt(jsonFieldsCountField.getText());

            // 禁用相关 UI 控件以防用户误操作
            producerTopicComboBox.setDisable(true);
            startAutoSendButton.setDisable(true);
            stopAutoSendButton.setDisable(false);
            messagesPerSecondField.setDisable(true);
            dataTypeChoiceBox.setDisable(true);
            keyLengthField.setDisable(true);
            jsonFieldsCountField.setDisable(true);
            sendButton.setDisable(true);

            log("开始向 Topic: \"" + topic + "\" 自动发送 " + messagesPerSecond + " msg/s...");

            // 初始化计数器
            sentCounts.put(topic, new AtomicLong(0));

            // 创建并安排定时任务
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleAtFixedRate(() -> {
                String key = generateRandomString(keyLength);
                String value;

                if ("JSON".equals(dataType)) {
                    value = generateRandomJson(jsonFieldsCount);
                } else { // String
                    value = generateRandomString(20); // 默认字符串长度为20
                }

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        sentCounts.get(topic).incrementAndGet();
                        Platform.runLater(() -> sentCountLabel.setText("已发送: " + sentCounts.get(topic).get()));
                    } else {
                        Platform.runLater(() -> log("发送消息失败: " + exception.getMessage()));
                    }
                });
            }, 0, intervalMs, TimeUnit.MILLISECONDS);

            autoSendExecutors.put(topic, executor);

        } catch (NumberFormatException e) {
            log("错误: 每秒消息数、长度或字段数必须是有效的数字。");
        }
    }

    public void stopAutoSend() {
        String topic = producerTopicComboBox.getValue();
        if (!autoSendExecutors.containsKey(topic)) {
            log("错误: Topic \"" + topic + "\" 的自动发送任务未在运行。");
            return;
        }

        ScheduledExecutorService executor = autoSendExecutors.get(topic);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        } finally {
            autoSendExecutors.remove(topic);
            sentCounts.remove(topic);
            log("Topic \"" + topic + "\" 的自动发送任务已停止。");

            // 恢复 UI 状态
            producerTopicComboBox.setDisable(false);
            startAutoSendButton.setDisable(false);
            stopAutoSendButton.setDisable(true);
            messagesPerSecondField.setDisable(false);
            dataTypeChoiceBox.setDisable(false);
            keyLengthField.setDisable(false);
            if ("JSON".equals(dataTypeChoiceBox.getValue())) {
                jsonFieldsCountField.setDisable(false);
                producerValueArea.setDisable(true);
            } else {
                jsonFieldsCountField.setDisable(true);
                producerValueArea.setDisable(false);
            }
            sendButton.setDisable(false);
            Platform.runLater(() -> sentCountLabel.setText("已发送: 0"));
        }
    }

    public void cleanup() {
        // 关闭所有自动发送任务
        if (!autoSendExecutors.isEmpty()) {
            log("正在关闭所有自动发送任务...");
            autoSendExecutors.values().forEach(executor -> {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                }
            });
            autoSendExecutors.clear();
            sentCounts.clear();
            log("所有自动发送任务已停止。");
        }
    }

    private String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(CHARACTERS.charAt(random.nextInt(CHARACTERS.length())));
        }
        return sb.toString();
    }

    private String generateRandomJson(int fieldCount) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < fieldCount; i++) {
            sb.append("\"key").append(i).append("\":\"").append(generateRandomString(8)).append("\"");
            if (i < fieldCount - 1) {
                sb.append(", ");
            }
        }
        sb.append("}");
        return sb.toString();
    }
}

package com.nan.kafkasimulator.controller;

import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;

import static com.nan.kafkasimulator.utils.Logger.log;

import java.net.URL;
import java.util.Properties;
import java.util.Random;
import java.util.ResourceBundle;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashMap;
import java.util.Map;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.nan.kafkasimulator.ControllerRegistry;

public class ProducerTabController implements Initializable {

    @FXML
    private TextField producerKeyField;
    @FXML
    private TextArea producerValueArea;
    @FXML
    private ChoiceBox<String> acksChoiceBox;
    @FXML
    private TextField batchSizeField;
    @FXML
    private TextField lingerMsField;
    @FXML
    private TextField bufferMemoryField;
    @FXML
    private Button onSendButtonClick;
    @FXML
    private TextField messagesPerSecondField;
    @FXML
    private ChoiceBox<String> dataTypeChoiceBox;
    @FXML
    private Label sentCountLabel;
    @FXML
    private TextField keyLengthField;
    @FXML
    private TextField jsonFieldsCountField;
    @FXML
    private Button startAutoSendButton;
    @FXML
    private Button stopAutoSendButton;

    private KafkaProducer<String, String> producer;
    private ScheduledExecutorService autoSendExecutor;
    private AtomicLong sentCount;
    private Map<TextField, ScheduledExecutorService> delayedListeners = new HashMap<>();
    private final String topicName;
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private final Random random = new Random();

    public ProducerTabController(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public void initialize(URL arg0, ResourceBundle arg1) {
        acksChoiceBox.getItems().addAll("all", "1", "0");
        acksChoiceBox.setValue("1");

        dataTypeChoiceBox.setItems(FXCollections.observableArrayList("String", "JSON"));
        dataTypeChoiceBox.setValue("String");
        dataTypeChoiceBox.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            jsonFieldsCountField.setDisable(!"JSON".equals(newVal));
            if ("JSON".equals(newVal)) {
                producerValueArea.setDisable(true);
                producerValueArea.setPromptText("JSON数据将自动生成");
            } else {
                producerValueArea.setDisable(false);
                producerValueArea.setPromptText("输入要发送的消息");
            }
        });

        sentCount = new AtomicLong(0);
        initializeProducer();

        // 添加配置变更监听器，任何改变都重新生成producer
        acksChoiceBox.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            initializeProducer();
        });

        // 添加延迟监听器，避免频繁触发初始化
        addDelayedListener(batchSizeField);
        addDelayedListener(lingerMsField);
        addDelayedListener(bufferMemoryField);
    }

    public KafkaProducer<String, String> getProducer() {
        if (producer == null) {
            initializeProducer();
        }
        return producer;
    }

    public void closeProducer() {
        if (producer != null) {
            producer.close(java.time.Duration.ofSeconds(5));
            producer = null;
            log(String.format("生产者 [%s] 已关闭。", topicName));
        }
    }

    private void initializeProducer() {
        log("初始化生产者...");
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ControllerRegistry.getConnectionManagerController().getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, acksChoiceBox.getValue());
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.parseInt(batchSizeField.getText()));
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, Integer.parseInt(lingerMsField.getText()));
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Long.parseLong(bufferMemoryField.getText()));

        this.producer = new KafkaProducer<>(producerProps);
    }

    void setStatusOnConnectionChanged(boolean connected) {
        if (connected) {
            startAutoSendButton.setDisable(false);
        } else {
            startAutoSendButton.setDisable(true);
            stopAutoSendButton.setDisable(true);
        }
    }

    // FXML 事件处理方法
    @FXML
    protected void onSendButtonClick() {
        sendMessage();
    }

    @FXML
    private void onStartAutoSendButtonClick() {
        startAutoSend();
    }

    @FXML
    private void onStopAutoSendButtonClick() {
        stopAutoSend();
    }

    public void setControlsDisable(boolean disable) {
        producerKeyField.setDisable(disable);
        producerValueArea.setDisable(disable);
        acksChoiceBox.setDisable(disable);
        batchSizeField.setDisable(disable);
        lingerMsField.setDisable(disable);
        bufferMemoryField.setDisable(disable);
        onSendButtonClick.setDisable(disable);
        messagesPerSecondField.setDisable(disable);
        dataTypeChoiceBox.setDisable(disable);
        sentCountLabel.setDisable(disable);
        keyLengthField.setDisable(disable);
        jsonFieldsCountField.setDisable(disable);
    }

    public void sendMessage() {
        if (getProducer() == null) {
            log("错误: 请先连接到 Kafka 集群。");
            return;
        }

        String key = producerKeyField.getText();
        String value = producerValueArea.getText();

        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

            log(String.format("正在发送消息到 [%s]...", topicName));
            producer.send(record, (metadata, exception) -> {
                Platform.runLater(() -> {
                    if (exception == null) {
                        log("消息发送成功！");
                        log(String.format("  - Topic: %s", metadata.topic()));
                        log(String.format("  - 分区: %d", metadata.partition()));
                        log(String.format("  - 偏移量: %d", metadata.offset()));
                    } else {
                        log(String.format("消息发送失败: %s", exception.getMessage()));
                    }
                });
            });
        } catch (Exception e) {
            log(String.format("消息发送失败: %s", e.getMessage()));
        }
    }

    public void startAutoSend() {
        if (getProducer() == null) {
            log("错误: 请先连接到 Kafka 集群。");
            return;
        }

        if (autoSendExecutor != null && !autoSendExecutor.isShutdown()) {
            log(String.format("错误: Topic %s 的自动发送任务已在运行。", topicName));
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
            startAutoSendButton.setDisable(true);
            stopAutoSendButton.setDisable(false);
            messagesPerSecondField.setDisable(true);
            dataTypeChoiceBox.setDisable(true);
            keyLengthField.setDisable(true);
            jsonFieldsCountField.setDisable(true);
            onSendButtonClick.setDisable(true);

            log(String.format("开始向 Topic: %s 自动发送 %s msg/s...", topicName, messagesPerSecond));

            // 重置计数器
            sentCount.set(0);

            // 创建并安排定时任务
            autoSendExecutor = Executors.newSingleThreadScheduledExecutor();
            autoSendExecutor.scheduleAtFixedRate(() -> {
                String key = generateRandomString(keyLength);
                String value;

                if ("JSON".equals(dataType)) {
                    value = generateRandomJson(jsonFieldsCount);
                } else { // String
                    value = generateRandomString(20); // 默认字符串长度为20
                }

                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        sentCount.incrementAndGet();
                        Platform.runLater(() -> sentCountLabel.setText(String.format("已发送: %d", sentCount.get())));
                    } else {
                        Platform.runLater(() -> log(String.format("发送消息失败: %s", exception.getMessage())));
                    }
                });
            }, 0, intervalMs, TimeUnit.MILLISECONDS);

        } catch (NumberFormatException e) {
            log("错误: 每秒消息数、长度或字段数必须是有效的数字。");
        }
    }

    public void stopAutoSend() {
        if (autoSendExecutor == null || autoSendExecutor.isShutdown()) {
            log(String.format("错误: Topic %s 的自动发送任务未在运行。", topicName));
            return;
        }

        autoSendExecutor.shutdown();
        try {
            if (!autoSendExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                autoSendExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            autoSendExecutor.shutdownNow();
        } finally {
            log(String.format("Topic %s 的自动发送任务已停止。", topicName));

            // 恢复 UI 状态
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
            onSendButtonClick.setDisable(false);
            Platform.runLater(() -> sentCountLabel.setText("已发送: 0"));
        }
    }

    private void addDelayedListener(TextField textField) {
        textField.textProperty().addListener(new ChangeListener<String>() {
            @Override
            public void changed(ObservableValue<? extends String> observable, String oldValue, String newValue) {
                // 取消之前可能存在的任务
                if (delayedListeners.containsKey(textField)) {
                    ScheduledExecutorService oldScheduler = delayedListeners.get(textField);
                    oldScheduler.shutdownNow();
                }

                // 创建新的调度器
                ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
                delayedListeners.put(textField, scheduler);

                // 延迟500毫秒执行初始化
                scheduler.schedule(() -> {
                    Platform.runLater(() -> initializeProducer());
                }, 500, TimeUnit.MILLISECONDS);
            }
        });
    }

    public void cleanup() {
        // 关闭所有延迟监听器
        for (ScheduledExecutorService scheduler : delayedListeners.values()) {
            scheduler.shutdownNow();
        }
        delayedListeners.clear();

        // 关闭自动发送任务
        if (autoSendExecutor != null && !autoSendExecutor.isShutdown()) {
            log(String.format("正在关闭 [%s] 的自动发送任务...", topicName));
            autoSendExecutor.shutdown();
            try {
                if (!autoSendExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    autoSendExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                autoSendExecutor.shutdownNow();
            }
            log(String.format("Topic [%s] 的自动发送任务已停止。", topicName));
        }

        // 关闭生产者
        closeProducer();
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
            sb.append(String.format("key %d : %s", i, generateRandomString(8)));
            if (i < fieldCount - 1) {
                sb.append(", ");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    public String getTopicName() {
        return topicName;
    }
}

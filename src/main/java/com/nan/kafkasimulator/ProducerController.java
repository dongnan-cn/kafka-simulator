package com.nan.kafkasimulator;

import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;

import static com.nan.kafkasimulator.utils.Logger.log;

import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.ResourceBundle;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.nan.kafkasimulator.utils.Alerter;

public class ProducerController implements Initializable {

    @FXML
    private ComboBox<String> producerTopicComboBox;
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

    private final Map<String, ScheduledExecutorService> autoSendExecutors = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> sentCounts = new ConcurrentHashMap<>();
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private final Random random = new Random();

    @Override
    public void initialize(URL arg0, ResourceBundle arg1) {
        acksChoiceBox.getItems().addAll("all", "1", "0");
        acksChoiceBox.setValue("1");
        startAutoSendButton.setDisable(true);
        stopAutoSendButton.setDisable(true);

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
            log("生产者已关闭。");
        }
    }

    private void initializeProducer() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ControllerRegistry.getConnectionManagerController().getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "1"); // 默认值

        this.producer = new KafkaProducer<>(producerProps);
    }

    // 获取生产者Topic ComboBox的方法，供其他控制器使用
    public ComboBox<String> getProducerTopicComboBox() {
        return producerTopicComboBox;
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

    @FXML
    protected void onProducerConfigChange() {
        updateProducerConfig(
                acksChoiceBox.getValue(),
                batchSizeField.getText(),
                lingerMsField.getText());

    }

    public void updateProducerConfig(String acks, String batchSize, String lingerMs) {

        try {
            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    ControllerRegistry.getConnectionManagerController().getBootstrapServers());
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.ACKS_CONFIG, acks);
            producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.parseInt(batchSize));
            producerProps.put(ProducerConfig.LINGER_MS_CONFIG, Integer.parseInt(lingerMs));

            // 关闭旧的生产者
            if (producer != null) {
                producer.close(java.time.Duration.ofSeconds(1));
            }
            // 创建新的生产者
            this.producer = new KafkaProducer<>(producerProps);
            log("生产者配置已更新");
        } catch (NumberFormatException e) {
            log("错误: 批次大小和延迟时间必须是有效的数字。");
            Alerter.showAlert("输入错误", null, "批次大小和延迟时间必须是有效的数字。");
        }
    }
    
    public void setControlsDisable(boolean disable) {
        producerTopicComboBox.setDisable(disable);
        producerKeyField.setDisable(disable);
        producerValueArea.setDisable(disable);
        acksChoiceBox.setDisable(disable);
        batchSizeField.setDisable(disable);
        lingerMsField.setDisable(disable);
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
            getProducer().send(record, (metadata, exception) -> {
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
        if (getProducer() == null || topic == null || topic.isEmpty()) {
            log("错误: 请先连接到 Kafka 并选择一个 Topic。");
            if (getProducer() == null) {
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
            onSendButtonClick.setDisable(true);

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
            onSendButtonClick.setDisable(false);
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
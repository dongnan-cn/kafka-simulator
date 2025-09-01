package com.nan.kafkasimulator.controller;

import com.nan.kafkasimulator.ControllerRegistry;
import com.nan.kafkasimulator.avro.SchemaManager;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.Modality;
import javafx.stage.Stage;

import static com.nan.kafkasimulator.utils.Logger.log;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.Random;
import java.util.ResourceBundle;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

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

    // Avro相关控件
    @FXML
    private HBox avroSchemaHBox;
    @FXML
    private ChoiceBox<String> avroSchemaChoiceBox;
    @FXML
    private Button manageSchemaButton;
    @FXML
    private Button generateMessageButton;
    @FXML
    private VBox avroMessageVBox;
    @FXML
    private TextArea avroMessageArea;

    private KafkaProducer<String, String> producer;
    private ScheduledExecutorService autoSendExecutor;
    private AtomicLong sentCount;
    private Map<TextField, ScheduledExecutorService> delayedListeners = new HashMap<>();
    private final String topicName;
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private final Random random = new Random();

    // Avro相关
    private SchemaManager schemaManager;

    public ProducerTabController(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public void initialize(URL arg0, ResourceBundle arg1) {
        // 初始化SchemaManager
        schemaManager = SchemaManager.getInstance();

        acksChoiceBox.getItems().addAll("all", "1", "0");
        acksChoiceBox.setValue("1");

        // 添加Avro数据类型选项
        dataTypeChoiceBox.setItems(FXCollections.observableArrayList("String", "JSON", "Avro"));
        dataTypeChoiceBox.setValue("String");
        dataTypeChoiceBox.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            jsonFieldsCountField.setDisable(!"JSON".equals(newVal));

            // 根据数据类型显示/隐藏相关UI控件
            if ("Avro".equals(newVal)) {
                // 显示Avro相关控件
                avroSchemaHBox.setVisible(true);
                avroSchemaHBox.setManaged(true);
                avroMessageVBox.setVisible(true);
                avroMessageVBox.setManaged(true);
                // 禁用普通消息区域
                producerValueArea.setDisable(true);
                producerValueArea.setPromptText("使用Avro格式发送消息");
            } else {
                // 隐藏Avro相关控件
                avroSchemaHBox.setVisible(false);
                avroSchemaHBox.setManaged(false);
                avroMessageVBox.setVisible(false);
                avroMessageVBox.setManaged(false);
                // 启用普通消息区域
                producerValueArea.setDisable(false);

                if ("JSON".equals(newVal)) {
                    producerValueArea.setDisable(true);
                    producerValueArea.setPromptText("JSON数据将自动生成");
                } else {
                    producerValueArea.setDisable(false);
                    producerValueArea.setPromptText("输入要发送的消息");
                }
            }
        });

        // 初始化Schema选择列表
        refreshSchemaList();

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

        // 根据选择的数据类型决定使用哪种序列化器
        String dataType = dataTypeChoiceBox.getValue();
        if ("Avro".equals(dataType)) {
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.nan.kafkasimulator.avro.AvroSerializer");
        } else {
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        }

        producerProps.put(ProducerConfig.ACKS_CONFIG, acksChoiceBox.getValue());
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.parseInt(batchSizeField.getText().isEmpty() ? "0" : batchSizeField.getText()));
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, Integer.parseInt(lingerMsField.getText().isEmpty() ? "0" : lingerMsField.getText()));
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Long.parseLong(bufferMemoryField.getText().isEmpty() ? "0" : bufferMemoryField.getText()));

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
        String value;
        String dataType = dataTypeChoiceBox.getValue();

        try {
            if ("Avro".equals(dataType)) {
                // 处理Avro消息
                String schemaName = avroSchemaChoiceBox.getValue();
                if (schemaName == null || schemaName.trim().isEmpty()) {
                    log("错误: 请选择一个Avro Schema");
                    return;
                }

                String avroMessage = avroMessageArea.getText();
                if (avroMessage == null || avroMessage.trim().isEmpty()) {
                    log("错误: 请输入Avro消息内容");
                    return;
                }

                // 使用Avro序列化器发送消息
                // 格式为: AVRO:schemaName:jsonContent
                // 序列化器会将其转换为真正的Avro二进制格式
                value = "AVRO:" + schemaName + ":" + avroMessage;
            } else {
                // 处理普通消息
                value = producerValueArea.getText();
            }

            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

            log(String.format("正在发送%s消息到 [%s]...", dataType, topicName));
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

            // 如果是Avro类型，还需要禁用Avro相关控件
            if ("Avro".equals(dataType)) {
                avroSchemaChoiceBox.setDisable(true);
                avroMessageArea.setDisable(true);
            }

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
                } else if ("Avro".equals(dataType)) {
                    // 处理Avro消息
                    String schemaName = avroSchemaChoiceBox.getValue();
                    String avroMessage;

                    // 如果消息区域为空，则根据Schema自动生成随机消息
                    if (avroMessageArea.getText() == null || avroMessageArea.getText().trim().isEmpty()) {
                        avroMessage = generateRandomAvroJson(schemaName);
                    } else {
                        avroMessage = avroMessageArea.getText();
                    }

                    // 使用Avro序列化器发送消息
                    // 格式为: AVRO:schemaName:jsonContent
                    // 序列化器会将其转换为真正的Avro二进制格式
                    value = "AVRO:" + schemaName + ":" + avroMessage;
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

            String dataType = dataTypeChoiceBox.getValue();
            if ("JSON".equals(dataType)) {
                jsonFieldsCountField.setDisable(false);
                producerValueArea.setDisable(true);
            } else if ("Avro".equals(dataType)) {
                // 恢复Avro相关控件状态
                avroSchemaChoiceBox.setDisable(false);
                avroMessageArea.setDisable(false);
                producerValueArea.setDisable(true);
                jsonFieldsCountField.setDisable(true);
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
                }, 1, TimeUnit.SECONDS);
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

    /**
     * 根据Avro Schema生成随机的JSON消息
     * @param schemaName Schema名称
     * @return 符合Schema的随机JSON消息
     */
    private String generateRandomAvroJson(String schemaName) {
        try {
            SchemaManager.SchemaVersion schemaVersion = schemaManager.getSchemaVersion(schemaName);
            if (schemaVersion == null) {
                log("错误: 找不到Schema: " + schemaName);
                return "{}";
            }

            org.apache.avro.Schema schema = schemaVersion.getSchema();
            return generateRandomJsonForSchema(schema);
        } catch (Exception e) {
            log("生成Avro JSON消息失败: " + e.getMessage());
            return "{}";
        }
    }

    /**
     * 根据Avro Schema生成随机的JSON消息
     * @param schema Avro Schema
     * @return 符合Schema的随机JSON消息
     */
    private String generateRandomJsonForSchema(org.apache.avro.Schema schema) {
        StringBuilder sb = new StringBuilder("{");
        List<org.apache.avro.Schema.Field> fields = schema.getFields();

        for (int i = 0; i < fields.size(); i++) {
            org.apache.avro.Schema.Field field = fields.get(i);
            String fieldName = field.name();
            org.apache.avro.Schema fieldSchema = field.schema();

            sb.append(String.format("%s : %s", fieldName, generateRandomValueForSchema(fieldSchema)));

            if (i < fields.size() - 1) {
                sb.append(", ");
            }
        }

        sb.append("}");
        return sb.toString();
    }

    /**
     * 根据Avro Schema类型生成随机值
     * @param schema Avro Schema
     * @return 随机值的JSON表示
     */
    private String generateRandomValueForSchema(org.apache.avro.Schema schema) {
        switch (schema.getType()) {
            case STRING:
                return generateRandomString(8);
            case INT:
                return String.valueOf(random.nextInt(1000));
            case LONG:
                return String.valueOf(random.nextLong() % 10000);
            case FLOAT:
                return String.valueOf(random.nextFloat() * 1000);
            case DOUBLE:
                return String.valueOf(random.nextDouble() * 10000);
            case BOOLEAN:
                return random.nextBoolean() ? "true" : "false";
            case NULL:
                return "null";
            case ARRAY:
                return generateRandomArray(schema.getElementType());
            case MAP:
                return generateRandomMap(schema.getValueType());
            case RECORD:
                return generateRandomJsonForSchema(schema);
            case UNION:
                // 对于联合类型，随机选择一种类型
                List<org.apache.avro.Schema> types = schema.getTypes();
                if (!types.isEmpty()) {
                    org.apache.avro.Schema selectedType = types.get(random.nextInt(types.size()));
                    return generateRandomValueForSchema(selectedType);
                }
                return "null";
            default:
                return "null";
        }
    }

    /**
     * 生成随机数组
     * @param elementSchema 元素类型
     * @return 随机数组的JSON表示
     */
    private String generateRandomArray(org.apache.avro.Schema elementSchema) {
        int size = random.nextInt(5) + 1; // 1-5个元素
        StringBuilder sb = new StringBuilder("[");

        for (int i = 0; i < size; i++) {
            sb.append(generateRandomValueForSchema(elementSchema));
            if (i < size - 1) {
                sb.append(", ");
            }
        }

        sb.append("]");
        return sb.toString();
    }

    /**
     * 生成随机Map
     * @param valueSchema 值类型
     * @return 随机Map的JSON表示
     */
    private String generateRandomMap(org.apache.avro.Schema valueSchema) {
        int size = random.nextInt(3) + 1; // 1-3个键值对
        StringBuilder sb = new StringBuilder("{");

        for (int i = 0; i < size; i++) {
            sb.append(String.format("key%d : %s", i, generateRandomValueForSchema(valueSchema)));
            if (i < size - 1) {
                sb.append(", ");
            }
        }

        sb.append("}");
        return sb.toString();
    }

    public String getTopicName() {
        return topicName;
    }

    /**
     * 刷新Schema列表
     */
    private void refreshSchemaList() {
        avroSchemaChoiceBox.setItems(FXCollections.observableArrayList(schemaManager.getAllSchemas().keySet()));
        if (!avroSchemaChoiceBox.getItems().isEmpty()) {
            avroSchemaChoiceBox.setValue(avroSchemaChoiceBox.getItems().get(0));
        }
    }

    /**
     * 打开Schema管理对话框
     */
    @FXML
    private void onManageSchemaButtonClick() {
        try {
            // 加载FXML
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/com/nan/kafkasimulator/fxml/schema-management.fxml"));
            Parent root = loader.load();

            // 创建对话框
            Stage dialogStage = new Stage();
            dialogStage.setTitle("Schema管理");
            dialogStage.initModality(Modality.APPLICATION_MODAL);
            dialogStage.setScene(new Scene(root));

            // 显示对话框并等待关闭
            dialogStage.showAndWait();

            // 对话框关闭后刷新Schema列表
            refreshSchemaList();
        } catch (IOException e) {
            log("打开Schema管理对话框失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 生成随机消息按钮点击事件处理
     */
    @FXML
    private void onGenerateMessageButtonClick() {
        String schemaName = avroSchemaChoiceBox.getValue();
        if (schemaName == null || schemaName.trim().isEmpty()) {
            log("错误: 请先选择一个Avro Schema");
            return;
        }

        String randomJson = generateRandomAvroJson(schemaName);
        avroMessageArea.setText(randomJson);
        log("已根据Schema '" + schemaName + "' 生成随机JSON消息");
    }
}

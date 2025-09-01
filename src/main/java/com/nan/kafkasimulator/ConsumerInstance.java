package com.nan.kafkasimulator;

import javafx.application.Platform;
import javafx.scene.control.TextArea;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import static com.nan.kafkasimulator.utils.Logger.log;

public class ConsumerInstance implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private final String instanceId;
    private final List<String> topicNames;
    private final TextArea messagesArea;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private ScheduledExecutorService commitScheduler;
    private final boolean autoCommit;
    private final int autoCommitInterval;

    public ConsumerInstance(Properties props, String instanceId, List<String> topicNames, TextArea messagesArea) {
        this.consumer = new KafkaConsumer<>(props);
        this.instanceId = instanceId;
        this.topicNames = topicNames;
        this.messagesArea = messagesArea;
        this.autoCommit = Boolean.TRUE.equals(props.get("enable.auto.commit"));
        // 安全地获取自动提交间隔，如果不存在或autoCommit为false，则使用默认值
        if (autoCommit && props.get("auto.commit.interval.ms") != null) {
            this.autoCommitInterval = Integer.parseInt(props.get("auto.commit.interval.ms").toString());
        } else {
            this.autoCommitInterval = 5000; // 默认值
        }

        // 如果启用了自动提交，设置定时提交任务
        if (autoCommit) {
            setupAutoCommit();
        }
    }

    @Override
    public void run() {
        log("消费者实例 '" + instanceId + "' 正在启动...");
        try {
            consumer.subscribe(topicNames);
            log("消费者实例 '" + instanceId + "' 已订阅 Topic: " + topicNames);
            log("消费者实例 '" + instanceId + "' 开始轮询消息。");

            while (running.get()) {

                if (paused.get()) {
                    log("消费者实例 '" + instanceId + "' 正在等待恢复...");
                    // 暂停 Kafka consumer，避免在恢复前继续拉取消息
                    consumer.pause(consumer.assignment());
                    // 进入一个短暂的循环，等待被唤醒或恢复
                    while (paused.get() && running.get()) {
                        try {
                            Thread.sleep(200);
                        } catch (InterruptedException e) {
                            // 忽略，继续检查状态
                        }
                    }
                    consumer.resume(consumer.assignment());
                    log("消费者实例 '" + instanceId + "' 已恢复拉取。");
                }

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                // 检查是否有手动提交请求
                if (requestManualCommit.get()) {
                    doManualCommit();
                }

                if (!records.isEmpty()) {
                    log("消费者实例 '" + instanceId + "' 收到 " + records.count() + " 条消息。");
                    Platform.runLater(() -> {
                        for (ConsumerRecord<String, String> record : records) {
                            String value = record.value();

                            // 检查是否是Avro消息（已转换为JSON格式）
                            String messageType = "普通";
                            if (value != null && value.startsWith("{") && value.endsWith("}")) {
                                try {
                                    // 尝试解析为JSON，如果是有效的JSON，则可能是Avro消息转换后的结果
                                    com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                                    mapper.readTree(value);
                                    messageType = "Avro";
                                } catch (Exception e) {
                                    // 不是有效的JSON，当作普通消息处理
                                }
                            }

                            String message = String.format(
                                    "消费者: %s | Topic: %s | 分区: %d | 偏移量: %d | 类型: %s | Key: %s | Value: %s%n",
                                    instanceId, record.topic(), record.partition(), record.offset(), 
                                    messageType, record.key(), value);
                            messagesArea.appendText(message);
                            messagesArea.setScrollTop(Double.MAX_VALUE);
                        }
                    });
                }
            }
        } catch (WakeupException e) {
            log("消费者实例 '" + instanceId + "' 被唤醒并即将关闭。");
            // 这是预期的关闭异常，忽略
        } finally {
            consumer.close();
            log("消费者实例 '" + instanceId + "' 已关闭。");
        }
    }

    private void setupAutoCommit() {
        commitScheduler = Executors.newSingleThreadScheduledExecutor();
        commitScheduler.scheduleAtFixedRate(() -> {
            if (running.get() && !paused.get()) {
                consumer.commitAsync((offsets, exception) -> {
                    if (exception == null) {
                        log("消费者实例 '" + instanceId + "' 已自动提交偏移量: " + offsets);
                    } else {
                        log("消费者实例 '" + instanceId + "' 自动提交偏移量失败: " + exception.getMessage());
                    }
                });
            }
        }, autoCommitInterval, autoCommitInterval, TimeUnit.MILLISECONDS);

        log("消费者实例 '" + instanceId + "' 已启用自动提交，提交间隔: " + autoCommitInterval + "ms");
    }

    /**
     * 手动提交偏移量的标志位
     */
    private final AtomicBoolean requestManualCommit = new AtomicBoolean(false);

    /**
     * 请求手动提交偏移量（线程安全）
     */
    public void requestManualCommit() {
        requestManualCommit.set(true);
    }

    /**
     * 执行手动提交偏移量（在消费者线程中调用）
     */
    private void doManualCommit() {
        if (running.get() && !paused.get()) {
            consumer.commitAsync((offsets, exception) -> {
                if (exception == null) {
                    log("消费者实例 '" + instanceId + "' 手动提交偏移量成功: " + offsets);
                } else {
                    log("消费者实例 '" + instanceId + "' 手动提交偏移量失败: " + exception.getMessage());
                }
            });
            requestManualCommit.set(false);
        } else {
            log("消费者实例 '" + instanceId + "' 当前无法提交偏移量（运行状态: " + running.get() + ", 暂停状态: " + paused.get() + "）");
        }
    }

    public void shutdown() {
        running.set(false);
        if (commitScheduler != null) {
            commitScheduler.shutdown();
            try {
                if (!commitScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    commitScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                commitScheduler.shutdownNow();
            }
        }
        consumer.wakeup(); // 中断 poll() 方法，以便线程可以退出
    }

    public void pause() {
        paused.set(true);
    }

    public void resume() {
        paused.set(false);
    }

    public String getInstanceId() {
        return instanceId;
    }

    public Set<TopicPartition> getAssignment() {
        return consumer.assignment();
    }
}
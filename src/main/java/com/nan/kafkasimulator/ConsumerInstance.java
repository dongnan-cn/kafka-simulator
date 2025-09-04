package com.nan.kafkasimulator;

import javafx.application.Platform;
import javafx.scene.control.TextArea;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.TopicPartition;
import com.nan.kafkasimulator.monitoring.MetricsCollector;
import com.nan.kafkasimulator.monitoring.MetricsCollectorSingleton;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
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
    private MetricsCollector metricsCollector;

    // 消息队列和清理机制相关字段
    private final ConcurrentLinkedQueue<String> messageQueue = new ConcurrentLinkedQueue<>();
    private final int maxMessageCount = 100; // 最大保留消息数量
    private ScheduledExecutorService cleanupScheduler; // 消息清理定时任务
    private final AtomicInteger currentMessageCount = new AtomicInteger(0); // 当前消息计数器

    public ConsumerInstance(Properties props, String instanceId, List<String> topicNames, TextArea messagesArea) {
        this.consumer = new KafkaConsumer<>(props);
        this.instanceId = instanceId;
        this.topicNames = topicNames;
        this.messagesArea = messagesArea;
        this.autoCommit = Boolean.TRUE.equals(props.get("enable.auto.commit"));

        // 初始化MetricsCollector
        this.metricsCollector = MetricsCollectorSingleton.getInstance();
        // Safely get auto commit interval, use default value if it doesn't exist or autoCommit is false
        if (autoCommit && props.get("auto.commit.interval.ms") != null) {
            this.autoCommitInterval = Integer.parseInt(props.get("auto.commit.interval.ms").toString());
        } else {
            this.autoCommitInterval = 5000; // Default value
        }

        // If auto commit is enabled, set up scheduled commit task
        if (autoCommit) {
            setupAutoCommit();
        }

        // Set up message cleanup task
        setupMessageCleanup();
    }

    @Override
    public void run() {
        log("Consumer instance '" + instanceId + "' is starting...");
        try {
            consumer.subscribe(topicNames);
            log("Consumer instance '" + instanceId + "' subscribed to Topics: " + topicNames);
            log("Consumer instance '" + instanceId + "' starts polling messages.");

            while (running.get()) {

                if (paused.get()) {
                    log("Consumer instance '" + instanceId + "' is waiting to resume...");
                    // Pause Kafka consumer to avoid continuing to pull messages before resuming
                    consumer.pause(consumer.assignment());
                    // Enter a short loop, waiting to be awakened or resumed
                    while (paused.get() && running.get()) {
                        try {
                            Thread.sleep(200);
                        } catch (InterruptedException e) {
                            // Ignore, continue checking status
                        }
                    }
                    consumer.resume(consumer.assignment());
                    log("Consumer instance '" + instanceId + "' has resumed pulling.");
                }

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                // Check if there is a manual commit request
                if (requestManualCommit.get()) {
                    doManualCommit();
                }

                if (!records.isEmpty()) {
                    log("Consumer instance '" + instanceId + "' received " + records.count() + " messages.");
                    Platform.runLater(() -> {
                        for (ConsumerRecord<String, String> record : records) {
                            String value = record.value();

                            // Check if it's an Avro message (already converted to JSON format)
                            String messageType = "Normal";
                            if (value != null && value.startsWith("{") && value.endsWith("}")) {
                                try {
                                    // Try to parse as JSON, if it's valid JSON, it might be the result of Avro message conversion
                                    com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                                    mapper.readTree(value);
                                    messageType = "Avro";
                                } catch (Exception e) {
                                    // Not valid JSON, treat as normal message
                                }
                            }

                            String message = String.format(
                                    "Consumer: %s | Topic: %s | Partition: %d | Offset: %d | Type: %s | Key: %s | Value: %s%n",
                                    instanceId, record.topic(), record.partition(), record.offset(),
                                    messageType, record.key(), value);

                            // 添加消息到队列
                            addMessageToQueue(message);

                            // 更新监控数据
                            if (metricsCollector != null) {
                                metricsCollector.updateConsumerThroughput(record.topic(), instanceId, 1.0);
                                // 简单计算延迟，实际应用中应该使用更精确的方法
                                long currentTime = System.currentTimeMillis();
                                long messageTimestamp = record.timestamp();
                                long latency = currentTime - messageTimestamp;
                                metricsCollector.updateTopicLatency(record.topic(), latency, latency, latency); // 使用相同的值作为P50、P95和P99
                            }
                        }
                    });
                }
            }
        } catch (WakeupException e) {
            log("Consumer instance '" + instanceId + "' was awakened and is about to close.");
            // This is an expected shutdown exception, ignore
        } finally {
            consumer.close();
            log("Consumer instance '" + instanceId + "' has been closed.");
        }
    }

    private void setupAutoCommit() {
        commitScheduler = Executors.newSingleThreadScheduledExecutor();
        commitScheduler.scheduleAtFixedRate(() -> {
            if (running.get() && !paused.get()) {
                consumer.commitAsync((offsets, exception) -> {
                    if (exception == null) {
                        log("Consumer instance '" + instanceId + "' has auto-committed offsets: " + offsets);
                    } else {
                        log("Consumer instance '" + instanceId + "' failed to auto-commit offsets: " + exception.getMessage());
                    }
                });
            }
        }, autoCommitInterval, autoCommitInterval, TimeUnit.MILLISECONDS);

        log("Consumer instance '" + instanceId + "' has enabled auto commit, commit interval: " + autoCommitInterval + "ms");
    }

    /**
     * Flag for manual commit of offsets
     */
    private final AtomicBoolean requestManualCommit = new AtomicBoolean(false);

    /**
     * Request manual commit of offsets (thread-safe)
     */
    public void requestManualCommit() {
        requestManualCommit.set(true);
    }

    /**
     * Execute manual commit of offsets (called in consumer thread)
     */
    private void doManualCommit() {
        if (running.get() && !paused.get()) {
            consumer.commitAsync((offsets, exception) -> {
                if (exception == null) {
                    log("Consumer instance '" + instanceId + "' manually committed offsets successfully: " + offsets);
                } else {
                    log("Consumer instance '" + instanceId + "' failed to manually commit offsets: " + exception.getMessage());
                }
            });
            requestManualCommit.set(false);
        } else {
            log("Consumer instance '" + instanceId + "' cannot commit offsets currently (running status: " + running.get() + ", paused status: " + paused.get() + ")");
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

        // 停止消息清理任务
        if (cleanupScheduler != null) {
            cleanupScheduler.shutdown();
            try {
                if (!cleanupScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    cleanupScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                cleanupScheduler.shutdownNow();
            }
        }

        consumer.wakeup(); // Interrupt poll() method so that the thread can exit
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

    /**
     * 设置消息清理任务
     */
    private void setupMessageCleanup() {
        cleanupScheduler = Executors.newSingleThreadScheduledExecutor();
        // 每30秒执行一次消息清理
        cleanupScheduler.scheduleAtFixedRate(this::cleanupOldMessages, 30, 30, TimeUnit.SECONDS);
        log("Consumer instance '" + instanceId + "' has enabled message cleanup task.");
    }

    /**
     * 添加消息到队列
     * @param message 消息内容
     */
    private void addMessageToQueue(String message) {
        messageQueue.offer(message);
        int count = currentMessageCount.incrementAndGet();

        // 如果消息数量超过最大值，立即触发清理
        if (count > maxMessageCount) {
            cleanupOldMessages();
        }

        // 更新TextArea显示
        updateMessagesDisplay();
    }

    /**
     * 清理旧消息
     */
    private void cleanupOldMessages() {
        int currentCount = currentMessageCount.get();

        // 如果当前消息数量超过最大值的80%，则清理一部分消息
        if (currentCount > maxMessageCount * 0.8) {
            int removeCount = (int) (currentCount * 0.3); // 移除30%的消息

            Platform.runLater(() -> {
                for (int i = 0; i < removeCount && !messageQueue.isEmpty(); i++) {
                    String message = messageQueue.poll();
                    if (message != null) {
                        currentMessageCount.decrementAndGet();
                    }
                }

                // 重新构建TextArea内容
                updateMessagesDisplay();

                log("Consumer instance '" + instanceId + "' has cleaned up " + removeCount + " old messages.");
            });
        }
    }

    /**
     * 更新消息显示
     */
    private void updateMessagesDisplay() {
        Platform.runLater(() -> {
            // 清空TextArea
            messagesArea.clear();

            // 将队列中的所有消息重新添加到TextArea
            for (String message : messageQueue) {
                messagesArea.appendText(message);
            }

            // 滚动到底部
            messagesArea.setScrollTop(Double.MAX_VALUE);
        });
    }
}

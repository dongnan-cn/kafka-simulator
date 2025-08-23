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
import java.util.concurrent.atomic.AtomicBoolean;
import static com.nan.kafkasimulator.utils.Logger.log;

public class ConsumerInstance implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private final String instanceId;
    private final List<String> topicNames;
    private final TextArea messagesArea;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean paused = new AtomicBoolean(false);

    public ConsumerInstance(Properties props, String instanceId, List<String> topicNames, TextArea messagesArea) {
        this.consumer = new KafkaConsumer<>(props);
        this.instanceId = instanceId;
        this.topicNames = topicNames;
        this.messagesArea = messagesArea;
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

                if (!records.isEmpty()) {
                    log("消费者实例 '" + instanceId + "' 收到 " + records.count() + " 条消息。");
                    Platform.runLater(() -> {
                        for (ConsumerRecord<String, String> record : records) {
                            String message = String.format(
                                    "消费者: %s | Topic: %s | 分区: %d | 偏移量: %d | Key: %s | Value: %s%n",
                                    instanceId, record.topic(), record.partition(), record.offset(), record.key(),
                                    record.value());
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

    public void shutdown() {
        running.set(false);
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
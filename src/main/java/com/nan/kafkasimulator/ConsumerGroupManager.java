package com.nan.kafkasimulator;

import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.scene.control.TextArea;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 封装一个 Kafka 消费者组的所有逻辑和状态。
 */
public class ConsumerGroupManager {
    private final String groupId;
    private final List<String> topics;
    private final boolean autoCommit;
    private final String bootstrapServers;

    private final TextArea messagesArea;
    private final TextArea partitionsArea;
    private final TextArea logArea;

    private final List<Thread> consumerThreads = new ArrayList<>();
    private final List<KafkaConsumer<String, String>> activeConsumers = new ArrayList<>();
    private volatile boolean isRunning = false;
    private int consumerCount = 0;

    public ConsumerGroupManager(String groupId, List<String> topics, boolean autoCommit, String bootstrapServers, TextArea messagesArea, TextArea partitionsArea, TextArea logArea) {
        this.groupId = groupId;
        this.topics = topics;
        this.autoCommit = autoCommit;
        this.bootstrapServers = bootstrapServers;
        this.messagesArea = messagesArea;
        this.partitionsArea = partitionsArea;
        this.logArea = logArea;
    }

    public String getGroupId() {
        return groupId;
    }

    public synchronized void start(int numConsumers) {
        if (isRunning) {
            log("消费者组 " + groupId + " 已经在运行中。");
            return;
        }
        isRunning = true;
        log("正在启动消费者组 " + groupId + "...");
        for (int i = 0; i < numConsumers; i++) {
            startNewConsumerInstance();
        }
    }

    public synchronized void startNewConsumerInstance() {
        if (!isRunning) {
            log("消费者组 " + groupId + " 未启动，无法添加新的消费者。");
            return;
        }

        consumerCount++;
        String clientId = "consumer-instance-" + groupId + "-" + consumerCount;

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        activeConsumers.add(consumer);
        consumer.subscribe(topics);

        Thread consumerThread = new Thread(() -> {
            try {
                log("消费者实例 " + clientId + " 正在加入消费者组 " + groupId + " 并开始轮询...");
                while (isRunning && !Thread.currentThread().isInterrupted()) {
                    try {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                        if (!records.isEmpty()) {
                            log("消费者实例 " + clientId + " 收到 " + records.count() + " 条消息。");
                        }
                        for (ConsumerRecord<String, String> record : records) {
                            String message = String.format(
                                    "消费者: %s | Topic: %s | 分区: %d | 偏移量: %d | Key: %s | Value: %s%n",
                                    clientId, record.topic(), record.partition(), record.offset(), record.key(), record.value());
                            Platform.runLater(() -> messagesArea.appendText(message));
                        }
                    } catch (org.apache.kafka.common.errors.InterruptException e) {
                        Thread.currentThread().interrupt(); // 重新设置中断标志
                        break;
                    }
                }
            } catch (Exception e) {
                log("消费者轮询出错: " + e.getMessage());
            } finally {
                // 优雅关闭消费者
                try {
                    consumer.close(Duration.ofSeconds(5));
                    log("消费者实例 " + clientId + " 已关闭。");
                } catch (Exception e) {
                    log("关闭消费者实例出错: " + e.getMessage());
                }
            }
        });

        consumerThread.setDaemon(true);
        consumerThreads.add(consumerThread);
        consumerThread.start();
    }
    
    public synchronized void stopAll() {
        if (!isRunning) return;
        isRunning = false;
        log("正在停止消费者组 " + groupId + " 中的所有消费者实例...");

        for (Thread thread : consumerThreads) {
            if (thread.isAlive()) {
                thread.interrupt(); // 中断线程，使其退出轮询
            }
        }
        consumerThreads.clear();
        activeConsumers.clear();
        log("所有消费者实例停止指令已发送。");
    }

    public synchronized void showPartitionAssignments(AdminClient adminClient) {
        Platform.runLater(() -> partitionsArea.clear());
        
        Task<Void> assignmentTask = new Task<>() {
            @Override
            protected Void call() throws Exception {
                // 直接使用 try-catch-finally 块处理，不需要 re-throw
                try {
                    DescribeConsumerGroupsResult result = adminClient.describeConsumerGroups(Collections.singleton(groupId));
                    ConsumerGroupDescription description = result.describedGroups().get(groupId).get(10, TimeUnit.SECONDS);

                    StringBuilder sb = new StringBuilder();
                    sb.append("消费者组 '").append(groupId).append("' 的分区分配:\n");
                    if (description.members() == null || description.members().isEmpty()) {
                        sb.append("当前消费者组没有活跃成员或分配的分区。");
                    } else {
                        for (MemberDescription member : description.members()) {
                            String memberId = member.consumerId();
                            if (memberId == null || memberId.isEmpty()) {
                                memberId = member.clientId(); // 使用 clientId 作为后备
                            }

                            sb.append("-> 消费者 ").append(memberId).append(":\n");
                            List<String> assignedPartitions = member.assignment().topicPartitions().stream()
                                    .map(tp -> tp.topic() + "-" + tp.partition())
                                    .sorted()
                                    .collect(Collectors.toList());

                            if (assignedPartitions.isEmpty()) {
                                sb.append("   - 未分配任何分区。\n");
                            } else {
                                assignedPartitions.forEach(p -> sb.append("   - ").append(p).append("\n"));
                            }
                        }
                    }

                    Platform.runLater(() -> partitionsArea.setText(sb.toString()));
                } catch (ExecutionException e) {
                    // 捕获ExecutionException，其内部的真正异常是getCause()
                    // 并且在onFailed中处理
                    throw new RuntimeException("获取分区分配失败: ", e.getCause());
                }
                return null;
            }
        };

        assignmentTask.setOnFailed(event -> {
            Throwable e = assignmentTask.getException();
            Platform.runLater(() -> {
                log("获取分区分配失败: " + e.getMessage());
                partitionsArea.setText("获取分区分配失败: " + e.getMessage());
            });
        });
        new Thread(assignmentTask).start();
    }

    public boolean isRunning() {
        return isRunning;
    }
    
    private void log(String message) {
        Platform.runLater(() -> logArea.appendText(message + "\n"));
    }
}
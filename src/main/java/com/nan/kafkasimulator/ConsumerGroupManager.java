package com.nan.kafkasimulator;

import javafx.application.Platform;
import javafx.concurrent.Task;
import javafx.scene.control.TextArea;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;

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

    private final List<ConsumerInstance> consumers = new ArrayList<>();
    private ExecutorService executorService;
    private ExecutorService adminExecutor;
    private final AtomicInteger instanceCounter = new AtomicInteger(1);
    private volatile boolean isRunning = false;

    public ConsumerGroupManager(String groupId, List<String> topics, boolean autoCommit, String bootstrapServers,
            TextArea messagesArea, TextArea partitionsArea, TextArea logArea) {
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

    public synchronized void start(int numInstances) {
        if (isRunning) {
            log("消费者组 '" + groupId + "' 已经在运行。");
            return;
        }

        //基础消费者数量是numInstances，随着手动添加，还会更多，所以不能固定大小线程池
        executorService = new ThreadPoolExecutor(numInstances, Integer.MAX_VALUE,
                                      0L, TimeUnit.MILLISECONDS,
                                      new SynchronousQueue<Runnable>());

        adminExecutor = Executors.newFixedThreadPool(1);
        isRunning = true;
        for (int i = 0; i < numInstances; i++) {
            startNewConsumerInstance();
        }
    }

    public synchronized void startNewConsumerInstance() {
        if (!isRunning) {
            log("消费者组 '" + groupId + "' 未启动，无法添加新的消费者。");
            return;
        }
        String instanceId = groupId + "-instance-" + instanceCounter.getAndIncrement();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, instanceId);

        ConsumerInstance instance = new ConsumerInstance(props, instanceId, topics, messagesArea, logArea);
        consumers.add(instance);

        executorService.submit(instance);
        
        log("已为消费者组 '" + groupId + "' 启动新的消费者实例: " + instanceId);
    }

    public synchronized void stopAll() {
        if (!isRunning) return;
        isRunning = false;
        log("正在停止消费者组 '" + groupId + "' 中的所有消费者实例...");

        consumers.forEach(ConsumerInstance::shutdown);
        consumers.clear(); // 清空列表

        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            } finally {
                executorService = null;
            }
        }

        if (adminExecutor != null) {
            adminExecutor.shutdown();
            try {
                if (!adminExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    adminExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                adminExecutor.shutdownNow();
            } finally {
                adminExecutor = null;
            }
        }
        log("所有消费者实例停止指令已发送。");
    }

    public synchronized void resume() {
        if (!isRunning) {
            log("正在恢复消费者组 '" + groupId + "'...");
            start(1);
        } else {
            log("消费者组 '" + groupId + "' 已经在运行中。");
        }
    }

    public synchronized void showPartitionAssignments(AdminClient adminClient) {
        Platform.runLater(() -> partitionsArea.clear());

        Task<Void> assignmentTask = new Task<>() {
            @Override
            protected Void call() throws Exception {
                try {
                    DescribeConsumerGroupsResult result = adminClient
                            .describeConsumerGroups(Collections.singleton(groupId));
                    ConsumerGroupDescription description = result.describedGroups().get(groupId).get(10,
                            TimeUnit.SECONDS);

                    StringBuilder sb = new StringBuilder();
                    sb.append("消费者组 '").append(groupId).append("' 的分区分配:\n");
                    if (description.members() == null || description.members().isEmpty()) {
                        sb.append("当前消费者组没有活跃成员或分配的分区。");
                    } else {
                        for (MemberDescription member : description.members()) {
                            String memberId = member.consumerId();
                            if (memberId == null || memberId.isEmpty()) {
                                memberId = member.clientId();
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
        adminExecutor.submit(assignmentTask);
    }

    public boolean isRunning() {
        return isRunning;
    }

    private void log(String message) {
        Platform.runLater(() -> logArea.appendText(message + "\n"));
    }
}
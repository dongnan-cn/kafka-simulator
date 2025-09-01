package com.nan.kafkasimulator;

import javafx.scene.control.TextArea;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import static com.nan.kafkasimulator.utils.Logger.log;

/**
 * 封装一个 Kafka 消费者组的所有逻辑和状态。
 */
public class ConsumerGroupManager {
    private final String groupId;
    private final List<String> topics;
    private final boolean autoCommit;
    private final String autoCommitInterval;
    private final String bootstrapServers;

    private final TextArea messagesArea;

    private final List<ConsumerInstance> consumers = new ArrayList<>();
    private ExecutorService executorService;
    private ExecutorService adminExecutor;
    private final AtomicInteger instanceCounter = new AtomicInteger(1);
    private volatile boolean isRunning = false;

    public ConsumerGroupManager(String groupId, List<String> topics, boolean autoCommit, String autoCommitInterval, String bootstrapServers,
            TextArea messagesArea) {
        this.groupId = groupId;
        this.topics = topics;
        this.autoCommit = autoCommit;
        this.bootstrapServers = bootstrapServers;
        this.messagesArea = messagesArea;
        this.autoCommitInterval = autoCommitInterval;
    }

    public String getGroupId() {
        return groupId;
    }

    public synchronized void start(int numInstances) {
        if (isRunning) {
            log("消费者组 '" + groupId + "' 已经在运行。");
            return;
        }

        // 基础消费者数量是numInstances，随着手动添加，还会更多，所以不能固定大小线程池
        executorService = new ThreadPoolExecutor(numInstances, Integer.MAX_VALUE,
                0L, TimeUnit.MILLISECONDS,
                new SynchronousQueue<Runnable>());

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

        // 检查是否需要使用Avro反序列化器
        // 这里我们暂时使用字符串反序列化器，后续可以根据Topic配置动态选择
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.nan.kafkasimulator.avro.AvroDeserializer");
        // 禁用Kafka的自动提交，使用我们自己的定时提交机制
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // 设置自定义属性，以便ConsumerInstance可以读取
        props.put("enable.auto.commit", autoCommit);
        if (autoCommit) {
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
            props.put("auto.commit.interval.ms", autoCommitInterval);
        } else {
            // 即使禁用自动提交，也设置一个默认值，避免ConsumerInstance中解析错误
            props.put("auto.commit.interval.ms", "5000");
        }
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, instanceId);

        ConsumerInstance instance = new ConsumerInstance(props, instanceId, topics, messagesArea);
        consumers.add(instance);
        executorService.submit(instance);

        log("已为消费者组 '" + groupId + "' 启动新的消费者实例: " + instanceId);
    }

    public synchronized void stopAll() {
        if (!isRunning)
            return;
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

    public void showPartitionAssignments(AdminClient adminClient, TextArea partitionAssignmentTextArea) {
        try {
            // 获取消费者组的分区分配信息
            Map<String, org.apache.kafka.clients.admin.ConsumerGroupDescription> groupDescriptions = adminClient
                    .describeConsumerGroups(Collections.singletonList(groupId)).all().get();

            ConsumerGroupDescription groupDescription = groupDescriptions.get(groupId);
            if (groupDescription == null) {
                partitionAssignmentTextArea.setText("未找到消费者组 " + groupId + " 的信息");
                return;
            }

            // 构建分区分配信息字符串
            StringBuilder assignmentInfo = new StringBuilder();
            assignmentInfo.append("消费者组: ").append(groupId).append("\n");
            assignmentInfo.append("状态: ").append(groupDescription.state()).append("\n");
            assignmentInfo.append("协调器: ").append(groupDescription.coordinator()).append("\n\n");

            assignmentInfo.append("成员分配信息:\n");
            for (MemberDescription member : groupDescription.members()) {
                assignmentInfo.append("\n成员ID: ").append(member.consumerId()).append("\n");
                assignmentInfo.append("客户端ID: ").append(member.clientId()).append("\n");
                assignmentInfo.append("主机: ").append(member.host()).append("\n");
                assignmentInfo.append("分配的分区: ").append(member.assignment().topicPartitions()).append("\n");
            }

            // 将信息显示在指定的TextArea中
            partitionAssignmentTextArea.setText(assignmentInfo.toString());
        } catch (Exception e) {
            partitionAssignmentTextArea.setText("获取分区分配信息时出错: " + e.getMessage());
        }
    }

    public boolean isRunning() {
        return isRunning;
    }

    public void pauseAll() {
        log("正在停止消费者组 '" + groupId + "' 中的所有消费者实例...");
        consumers.forEach(ConsumerInstance::pause);
    }

    public void resumeAll() {
        log("正在停止消费者组 '" + groupId + "' 中的所有消费者实例...");
        consumers.forEach(ConsumerInstance::resume);
    }

    /**
     * 手动提交所有消费者实例的偏移量
     */
    public void manualCommitAll() {
        log("正在手动提交消费者组 '" + groupId + "' 中所有消费者实例的偏移量...");
        consumers.forEach(ConsumerInstance::requestManualCommit);
    }
}
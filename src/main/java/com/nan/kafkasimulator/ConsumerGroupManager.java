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
 * Encapsulates all logic and state of a Kafka consumer group.
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
            log("Consumer group '" + groupId + "' is already running.");
            return;
        }

        // Basic consumer count is numInstances, will be more with manual additions, so cannot use fixed size thread pool
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
            log("Consumer group '" + groupId + "' is not started, cannot add new consumer.");
            return;
        }
        String instanceId = groupId + "-instance-" + instanceCounter.getAndIncrement();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Check if Avro deserializer needs to be used
        // Here we temporarily use string deserializer, later can dynamically choose based on Topic configuration
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.nan.kafkasimulator.avro.AvroDeserializer");
        // Disable Kafka's auto commit, use our own timed commit mechanism
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // Set custom properties so that ConsumerInstance can read them
        props.put("enable.auto.commit", autoCommit);
        if (autoCommit) {
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
            props.put("auto.commit.interval.ms", autoCommitInterval);
        } else {
            // Even if auto commit is disabled, set a default value to avoid parsing errors in ConsumerInstance
            props.put("auto.commit.interval.ms", "5000");
        }
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, instanceId);

        ConsumerInstance instance = new ConsumerInstance(props, instanceId, topics, messagesArea);
        consumers.add(instance);
        executorService.submit(instance);

        log("Started new consumer instance for consumer group '" + groupId + "': " + instanceId);
    }

    public synchronized void stopAll() {
        if (!isRunning)
            return;
        isRunning = false;
        log("Stopping all consumer instances in consumer group '" + groupId + "'...");

        consumers.forEach(ConsumerInstance::shutdown);
        consumers.clear(); // Clear the list

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
        log("Stop commands have been sent to all consumer instances.");
    }

    public void showPartitionAssignments(AdminClient adminClient, TextArea partitionAssignmentTextArea) {
        try {
            // 获取消费者组的分区分配信息
            Map<String, org.apache.kafka.clients.admin.ConsumerGroupDescription> groupDescriptions = adminClient
                    .describeConsumerGroups(Collections.singletonList(groupId)).all().get();

            ConsumerGroupDescription groupDescription = groupDescriptions.get(groupId);
            if (groupDescription == null) {
                partitionAssignmentTextArea.setText("Information not found for consumer group " + groupId);
                return;
            }

            // 构建分区分配信息字符串
            StringBuilder assignmentInfo = new StringBuilder();
            assignmentInfo.append("Consumer Group: ").append(groupId).append("\n");
            assignmentInfo.append("State: ").append(groupDescription.state()).append("\n");
            assignmentInfo.append("Coordinator: ").append(groupDescription.coordinator()).append("\n\n");

            assignmentInfo.append("Member Assignment Information:\n");
            for (MemberDescription member : groupDescription.members()) {
                assignmentInfo.append("\nMember ID: ").append(member.consumerId()).append("\n");
                assignmentInfo.append("Client ID: ").append(member.clientId()).append("\n");
                assignmentInfo.append("Host: ").append(member.host()).append("\n");
                assignmentInfo.append("Assigned Partitions: ").append(member.assignment().topicPartitions()).append("\n");
            }

            // 将信息显示在指定的TextArea中
            partitionAssignmentTextArea.setText(assignmentInfo.toString());
        } catch (Exception e) {
            partitionAssignmentTextArea.setText("Error getting partition assignment information: " + e.getMessage());
        }
    }

    public boolean isRunning() {
        return isRunning;
    }

    public void pauseAll() {
        log("Stopping all consumer instances in consumer group '" + groupId + "'...");
        consumers.forEach(ConsumerInstance::pause);
    }

    public void resumeAll() {
        log("Resuming all consumer instances in consumer group '" + groupId + "'...");
        consumers.forEach(ConsumerInstance::resume);
    }

    /**
     * 手动提交所有消费者实例的偏移量
     */
    public void manualCommitAll() {
        log("Manually committing offsets for all consumer instances in consumer group '" + groupId + "'...");
        consumers.forEach(ConsumerInstance::requestManualCommit);
    }
}
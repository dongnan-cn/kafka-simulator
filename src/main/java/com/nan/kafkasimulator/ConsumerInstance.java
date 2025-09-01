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

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

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
                            messagesArea.appendText(message);
                            messagesArea.setScrollTop(Double.MAX_VALUE);
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
}
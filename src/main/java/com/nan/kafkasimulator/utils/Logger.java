package com.nan.kafkasimulator.utils;

import javafx.application.Platform;
import javafx.scene.control.TextArea;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Logger {
    private static Logger instance;
    private static TextArea logArea;
    
    // 日志队列和清理机制相关字段
    private static final ConcurrentLinkedQueue<String> logQueue = new ConcurrentLinkedQueue<>();
    private static final int maxLogCount = 100; // 最大保留日志数量
    private static ScheduledExecutorService cleanupScheduler; // 日志清理定时任务
    private static final AtomicInteger currentLogCount = new AtomicInteger(0); // 当前日志计数器

    private Logger() {
    }

    public static Logger getInstance() {
        if (instance == null) {
            instance = new Logger();
        }
        return instance;
    }

    public void initialize(TextArea area) {
        if (logArea != null) {
            throw new IllegalStateException("Logger has already been initialized.");
        }
        logArea = area;
        
        // 设置日志清理任务
        setupLogCleanup();
    }
    
    /**
     * 设置日志清理任务
     */
    private static void setupLogCleanup() {
        cleanupScheduler = Executors.newSingleThreadScheduledExecutor();
        // 每60秒执行一次日志清理
        cleanupScheduler.scheduleAtFixedRate(Logger::cleanupOldLogs, 60, 60, TimeUnit.SECONDS);
        log("Logger has enabled log cleanup task.");
    }

    public static void log(String message) {
        if (logArea == null) {
            System.err.println("Logger is not initialized! Message: " + message);
            return;
        }
        
        // 添加日志到队列
        addLogToQueue(message);
    }
    
    /**
     * 添加日志到队列
     * @param message 日志内容
     */
    private static void addLogToQueue(String message) {
        logQueue.offer(message);
        int count = currentLogCount.incrementAndGet();
        
        // 如果日志数量超过最大值，立即触发清理
        if (count > maxLogCount) {
            cleanupOldLogs();
        }
        
        // 更新TextArea显示
        updateLogDisplay();
    }
    
    /**
     * 清理旧日志
     */
    private static void cleanupOldLogs() {
        int currentCount = currentLogCount.get();
        
        // 如果当前日志数量超过最大值的80%，则清理一部分日志
        if (currentCount > maxLogCount * 0.8) {
            int removeCount = (int) (currentCount * 0.3); // 移除30%的日志
            
            Platform.runLater(() -> {
                for (int i = 0; i < removeCount && !logQueue.isEmpty(); i++) {
                    String log = logQueue.poll();
                    if (log != null) {
                        currentLogCount.decrementAndGet();
                    }
                }
                
                // 重新构建TextArea内容
                updateLogDisplay();
                
                // 记录清理操作（但不添加到队列，避免递归）
                if (logArea != null) {
                    String cleanupMessage = "Logger has cleaned up " + removeCount + " old log entries.";
                    logArea.appendText(cleanupMessage + "\n");
                    logArea.setScrollTop(Double.MAX_VALUE);
                }
            });
        }
    }
    
    /**
     * 更新日志显示
     */
    private static void updateLogDisplay() {
        Platform.runLater(() -> {
            if (logArea == null) return;
            
            // 清空TextArea
            logArea.clear();
            
            // 将队列中的所有日志重新添加到TextArea
            for (String log : logQueue) {
                logArea.appendText(log + "\n");
            }
            
            // 滚动到底部
            logArea.setScrollTop(Double.MAX_VALUE);
        });
    }
    
    /**
     * 关闭日志系统，释放资源
     */
    public static void shutdown() {
        // 停止日志清理任务
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
    }
}

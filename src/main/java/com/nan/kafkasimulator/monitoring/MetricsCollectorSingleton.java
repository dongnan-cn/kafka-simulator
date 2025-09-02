package com.nan.kafkasimulator.monitoring;

import java.util.logging.Logger;

/**
 * MetricsCollector单例类，确保整个应用程序使用同一个MetricsCollector实例
 */
public class MetricsCollectorSingleton {
    private static final Logger LOGGER = Logger.getLogger(MetricsCollectorSingleton.class.getName());

    private static MetricsCollector instance;

    private MetricsCollectorSingleton() {
        // 私有构造函数，防止实例化
    }

    /**
     * 获取MetricsCollector实例
     * @return MetricsCollector实例
     */
    public static synchronized MetricsCollector getInstance() {
        if (instance == null) {
            instance = new MetricsCollector();
            LOGGER.info("Created new MetricsCollector instance");
        }
        return instance;
    }

    /**
     * 重置MetricsCollector实例（主要用于测试）
     */
    public static synchronized void reset() {
        instance = null;
        LOGGER.info("Reset MetricsCollector instance");
    }
}

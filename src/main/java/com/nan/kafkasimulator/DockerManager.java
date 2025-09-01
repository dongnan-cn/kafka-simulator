package com.nan.kafkasimulator;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;

import java.util.List;
import static com.nan.kafkasimulator.utils.Logger.log;

/**
 * Docker容器管理类，用于操作Kafka容器
 */
public class DockerManager {
    private DockerClient dockerClient;
    private static DockerManager instance;

    private DockerManager() {
        try {
            // 创建Docker客户端配置
            DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder().build();

            // 创建HTTP客户端
            DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                    .dockerHost(config.getDockerHost())
                    .sslConfig(config.getSSLConfig())
                    .maxConnections(100)
                    .connectionTimeout(java.time.Duration.ofSeconds(30))
                    .responseTimeout(java.time.Duration.ofSeconds(45))
                    .build();

            // 创建Docker客户端
            dockerClient = DockerClientBuilder.getInstance(config)
                    .withDockerHttpClient(httpClient)
                    .build();

            log("Docker客户端初始化成功");
        } catch (Exception e) {
            log("Docker客户端初始化失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 获取DockerManager单例
     * @return DockerManager实例
     */
    public static synchronized DockerManager getInstance() {
        if (instance == null) {
            instance = new DockerManager();
        }
        return instance;
    }

    /**
     * 获取所有Kafka容器
     * @return Kafka容器列表
     */
    public List<Container> getKafkaContainers() {
        try {
            // 列出所有名称包含"kafka"的容器
            return dockerClient.listContainersCmd()
                    .withShowAll(true)
                    .withNameFilter(List.of("kafka"))
                    .exec();
        } catch (Exception e) {
            log("获取Kafka容器失败: " + e.getMessage());
            e.printStackTrace();
            return List.of();
        }
    }

    /**
     * 根据容器ID获取容器
     * @param containerId 容器ID
     * @return 容器对象
     */
    public Container getContainerById(String containerId) {
        try {
            List<Container> containers = dockerClient.listContainersCmd()
                    .withShowAll(true)
                    .withIdFilter(List.of(containerId))
                    .exec();
            return containers.isEmpty() ? null : containers.get(0);
        } catch (Exception e) {
            log("获取容器失败: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 根据容器名称获取容器
     * @param containerName 容器名称
     * @return 容器对象
     */
    public Container getContainerByName(String containerName) {
        try {
            List<Container> containers = dockerClient.listContainersCmd()
                    .withShowAll(true)
                    .withNameFilter(List.of(containerName))
                    .exec();
            return containers.isEmpty() ? null : containers.get(0);
        } catch (Exception e) {
            log("获取容器失败: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 停止容器
     * @param containerId 容器ID
     * @return 操作是否成功
     */
    public boolean stopContainer(String containerId) {
        try {
            dockerClient.stopContainerCmd(containerId).exec();
            log("已停止容器: " + containerId);
            return true;
        } catch (Exception e) {
            log("停止容器失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 启动容器
     * @param containerId 容器ID
     * @return 操作是否成功
     */
    public boolean startContainer(String containerId) {
        try {
            dockerClient.startContainerCmd(containerId).exec();
            log("已启动容器: " + containerId);
            return true;
        } catch (Exception e) {
            log("启动容器失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 检查容器是否运行中
     * @param containerId 容器ID
     * @return 容器是否运行中
     */
    public boolean isContainerRunning(String containerId) {
        try {
            Container container = getContainerById(containerId);
            return container != null && container.getState().equals("running");
        } catch (Exception e) {
            log("检查容器状态失败: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 关闭Docker客户端
     */
    public void close() {
        if (dockerClient != null) {
            try {
                dockerClient.close();
                log("Docker客户端已关闭");
            } catch (Exception e) {
                log("关闭Docker客户端失败: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}

package com.nan.kafkasimulator;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.nan.kafkasimulator.utils.Logger.log;

/**
 * 模拟Kafka broker故障的类
 */
public class BrokerFailureSimulator {
    private final AdminClient adminClient;
    private final DockerManager dockerManager;
    private final Map<Integer, Boolean> brokerStatusMap; // brokerId -> isActive (true=正常, false=宕机)
    private final Map<Integer, String> brokerHostMap; // brokerId -> host:port
    private final Map<Integer, String> brokerContainerMap; // brokerId -> containerId
    private final Map<Integer, Long> failureTimeMap; // brokerId -> 故障时间戳

    public BrokerFailureSimulator(AdminClient adminClient) {
        this.adminClient = adminClient;
        this.dockerManager = DockerManager.getInstance();
        this.brokerStatusMap = new HashMap<>();
        this.brokerHostMap = new HashMap<>();
        this.brokerContainerMap = new HashMap<>();
        this.failureTimeMap = new HashMap<>();
        initializeBrokerInfo();
    }

    /**
     * 初始化broker信息
     */
    private void initializeBrokerInfo() {
        try {
            DescribeClusterResult describeClusterResult = adminClient.describeCluster();
            Collection<Node> nodes = describeClusterResult.nodes().get(10, TimeUnit.SECONDS);

            // 获取所有Kafka容器
            List<com.github.dockerjava.api.model.Container> kafkaContainers = dockerManager.getKafkaContainers();

            for (Node node : nodes) {
                int brokerId = node.id();
                String host = node.host();
                int port = node.port();
                brokerStatusMap.put(brokerId, true); // 初始状态为正常
                brokerHostMap.put(brokerId, host + ":" + port);

                // 尝试找到对应的容器
                String containerName = "kafka" + brokerId;
                for (com.github.dockerjava.api.model.Container container : kafkaContainers) {
                    for (String name : container.getNames()) {
                        if (name.contains(containerName)) {
                            brokerContainerMap.put(brokerId, container.getId());
                            log("初始化Broker信息: ID=" + brokerId + ", 地址=" + host + ":" + port + ", 容器ID=" + container.getId());
                            break;
                        }
                    }
                }

                // 如果没有找到对应的容器，记录警告
                if (!brokerContainerMap.containsKey(brokerId)) {
                    log("警告: 未找到Broker " + brokerId + " 对应的Docker容器");
                }
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log("获取集群信息失败: " + e.getMessage());
        }
    }

    /**
     * 模拟broker宕机
     * @param brokerId 要宕机的broker ID
     * @return 操作是否成功
     */
    public boolean failBroker(int brokerId) {
        if (!brokerStatusMap.containsKey(brokerId)) {
            log("错误: Broker ID " + brokerId + " 不存在");
            return false;
        }

        if (!brokerStatusMap.get(brokerId)) {
            log("警告: Broker " + brokerId + " 已经处于宕机状态");
            return false;
        }

        // 检查是否有对应的容器
        String containerId = brokerContainerMap.get(brokerId);
        if (containerId == null) {
            log("错误: 未找到Broker " + brokerId + " 对应的Docker容器");
            return false;
        }

        // 实际停止容器
        boolean success = dockerManager.stopContainer(containerId);
        if (success) {
            brokerStatusMap.put(brokerId, false);
            failureTimeMap.put(brokerId, System.currentTimeMillis());
            log("已停止Broker " + brokerId + " (" + brokerHostMap.get(brokerId) + ") 对应的容器");
        } else {
            log("停止Broker " + brokerId + " 容器失败");
        }

        return success;
    }

    /**
     * 模拟broker恢复
     * @param brokerId 要恢复的broker ID
     * @return 操作是否成功
     */
    public boolean recoverBroker(int brokerId) {
        if (!brokerStatusMap.containsKey(brokerId)) {
            log("错误: Broker ID " + brokerId + " 不存在");
            return false;
        }

        if (brokerStatusMap.get(brokerId)) {
            log("警告: Broker " + brokerId + " 已经处于正常状态");
            return false;
        }

        // 检查是否有对应的容器
        String containerId = brokerContainerMap.get(brokerId);
        if (containerId == null) {
            log("错误: 未找到Broker " + brokerId + " 对应的Docker容器");
            return false;
        }

        // 实际启动容器
        boolean success = dockerManager.startContainer(containerId);
        if (success) {
            brokerStatusMap.put(brokerId, true);
            long downtime = System.currentTimeMillis() - failureTimeMap.get(brokerId);
            log("已启动Broker " + brokerId + " (" + brokerHostMap.get(brokerId) + ") 对应的容器，宕机时长: " + downtime + "ms");
        } else {
            log("启动Broker " + brokerId + " 容器失败");
        }

        return success;
    }

    /**
     * 获取broker状态
     * @param brokerId broker ID
     * @return broker状态 (true=正常, false=宕机)
     */
    public Boolean getBrokerStatus(int brokerId) {
        return brokerStatusMap.get(brokerId);
    }

    /**
     * 获取所有broker状态
     * @return 所有broker状态的映射
     */
    public Map<Integer, Boolean> getAllBrokerStatus() {
        return new HashMap<>(brokerStatusMap);
    }

    /**
     * 获取broker地址
     * @param brokerId broker ID
     * @return broker地址 (host:port)
     */
    public String getBrokerAddress(int brokerId) {
        return brokerHostMap.get(brokerId);
    }

    /**
     * 获取所有broker信息
     * @return 所有broker信息的列表
     */
    public List<BrokerInfo> getAllBrokerInfo() {
        List<BrokerInfo> brokerInfoList = new ArrayList<>();
        for (Map.Entry<Integer, Boolean> entry : brokerStatusMap.entrySet()) {
            int brokerId = entry.getKey();
            String address = brokerHostMap.get(brokerId);
            Long failureTime = failureTimeMap.get(brokerId);

            // 检查容器的实际运行状态
            boolean isActive;
            String containerId = brokerContainerMap.get(brokerId);
            if (containerId != null) {
                isActive = dockerManager.isContainerRunning(containerId);
                // 更新内部状态以匹配实际状态
                if (isActive != entry.getValue()) {
                    brokerStatusMap.put(brokerId, isActive);
                    if (!isActive && failureTime == null) {
                        failureTimeMap.put(brokerId, System.currentTimeMillis());
                    }
                }
            } else {
                // 如果没有容器ID，使用内部状态
                isActive = entry.getValue();
            }

            brokerInfoList.add(new BrokerInfo(brokerId, address, isActive, failureTime));
        }
        return brokerInfoList;
    }

    /**
     * Broker信息类
     */
    public static class BrokerInfo {
        private final int id;
        private final String address;
        private final boolean active;
        private final Long failureTime;

        public BrokerInfo(int id, String address, boolean active, Long failureTime) {
            this.id = id;
            this.address = address;
            this.active = active;
            this.failureTime = failureTime;
        }

        public int getId() {
            return id;
        }

        public String getAddress() {
            return address;
        }

        public boolean isActive() {
            return active;
        }

        public Long getFailureTime() {
            return failureTime;
        }

        @Override
        public String toString() {
            return "BrokerInfo{" +
                    "id=" + id +
                    ", address='" + address + '\'' +
                    ", active=" + active +
                    ", failureTime=" + (failureTime != null ? new Date(failureTime) : "N/A") +
                    '}';
        }
    }
}

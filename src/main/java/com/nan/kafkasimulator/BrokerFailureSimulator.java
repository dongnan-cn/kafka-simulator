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
 * Class for simulating Kafka broker failures
 */
public class BrokerFailureSimulator {
    private final AdminClient adminClient;
    private final DockerManager dockerManager;
    private final Map<Integer, Boolean> brokerStatusMap; // brokerId -> isActive (true=active, false=failed)
    private final Map<Integer, String> brokerHostMap; // brokerId -> host:port
    private final Map<Integer, String> brokerContainerMap; // brokerId -> containerId
    private final Map<Integer, Long> failureTimeMap; // brokerId -> failure timestamp

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
     * Initialize broker information
     */
    private void initializeBrokerInfo() {
        try {
            DescribeClusterResult describeClusterResult = adminClient.describeCluster();
            Collection<Node> nodes = describeClusterResult.nodes().get(10, TimeUnit.SECONDS);

            // Get all Kafka containers
            List<com.github.dockerjava.api.model.Container> kafkaContainers = dockerManager.getKafkaContainers();

            for (Node node : nodes) {
                int brokerId = node.id();
                String host = node.host();
                int port = node.port();
                brokerStatusMap.put(brokerId, true); // Initial state is active
                brokerHostMap.put(brokerId, host + ":" + port);

                // Try to find the corresponding container
                String containerName = "kafka" + brokerId;
                for (com.github.dockerjava.api.model.Container container : kafkaContainers) {
                    for (String name : container.getNames()) {
                        if (name.contains(containerName)) {
                            brokerContainerMap.put(brokerId, container.getId());
                            log("Initialize Broker info: ID=" + brokerId + ", Address=" + host + ":" + port + ", Container ID=" + container.getId());
                            break;
                        }
                    }
                }

                // If no corresponding container is found, log a warning
                if (!brokerContainerMap.containsKey(brokerId)) {
                    log("Warning: Docker container not found for Broker " + brokerId);
                }
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log("Failed to get cluster information: " + e.getMessage());
        }
    }

    /**
     * Simulate broker failure
     * @param brokerId ID of the broker to fail
     * @return whether the operation was successful
     */
    public boolean failBroker(int brokerId) {
        if (!brokerStatusMap.containsKey(brokerId)) {
            log("Error: Broker ID " + brokerId + " does not exist");
            return false;
        }

        if (!brokerStatusMap.get(brokerId)) {
            log("Warning: Broker " + brokerId + " is already in failed state");
            return false;
        }

        // Check if there is a corresponding container
        String containerId = brokerContainerMap.get(brokerId);
        if (containerId == null) {
            log("Error: Docker container not found for Broker " + brokerId);
            return false;
        }

        // Actually stop the container
        boolean success = dockerManager.stopContainer(containerId);
        if (success) {
            brokerStatusMap.put(brokerId, false);
            failureTimeMap.put(brokerId, System.currentTimeMillis());
            log("Stopped container for Broker " + brokerId + " (" + brokerHostMap.get(brokerId) + ")");
        } else {
            log("Failed to stop container for Broker " + brokerId);
        }

        return success;
    }

    /**
     * Simulate broker recovery
     * @param brokerId ID of the broker to recover
     * @return whether the operation was successful
     */
    public boolean recoverBroker(int brokerId) {
        if (!brokerStatusMap.containsKey(brokerId)) {
            log("Error: Broker ID " + brokerId + " does not exist");
            return false;
        }

        if (brokerStatusMap.get(brokerId)) {
            log("Warning: Broker " + brokerId + " is already in active state");
            return false;
        }

        // Check if there is a corresponding container
        String containerId = brokerContainerMap.get(brokerId);
        if (containerId == null) {
            log("Error: Docker container not found for Broker " + brokerId);
            return false;
        }

        // Actually start the container
        boolean success = dockerManager.startContainer(containerId);
        if (success) {
            brokerStatusMap.put(brokerId, true);
            long downtime = System.currentTimeMillis() - failureTimeMap.get(brokerId);
            log("Started container for Broker " + brokerId + " (" + brokerHostMap.get(brokerId) + "), downtime: " + downtime + "ms");
        } else {
            log("Failed to start container for Broker " + brokerId);
        }

        return success;
    }

    /**
     * Get broker status
     * @param brokerId broker ID
     * @return broker status (true=active, false=failed)
     */
    public Boolean getBrokerStatus(int brokerId) {
        return brokerStatusMap.get(brokerId);
    }

    /**
     * Get all broker statuses
     * @return map of all broker statuses
     */
    public Map<Integer, Boolean> getAllBrokerStatus() {
        return new HashMap<>(brokerStatusMap);
    }

    /**
     * Get broker address
     * @param brokerId broker ID
     * @return broker address (host:port)
     */
    public String getBrokerAddress(int brokerId) {
        return brokerHostMap.get(brokerId);
    }

    /**
     * Get all broker information
     * @return list of all broker information
     */
    public List<BrokerInfo> getAllBrokerInfo() {
        List<BrokerInfo> brokerInfoList = new ArrayList<>();
        for (Map.Entry<Integer, Boolean> entry : brokerStatusMap.entrySet()) {
            int brokerId = entry.getKey();
            String address = brokerHostMap.get(brokerId);
            Long failureTime = failureTimeMap.get(brokerId);

            // Check the actual running status of the container
            boolean isActive;
            String containerId = brokerContainerMap.get(brokerId);
            if (containerId != null) {
                isActive = dockerManager.isContainerRunning(containerId);
                // Update internal state to match actual state
                if (isActive != entry.getValue()) {
                    brokerStatusMap.put(brokerId, isActive);
                    if (!isActive && failureTime == null) {
                        failureTimeMap.put(brokerId, System.currentTimeMillis());
                    }
                }
            } else {
                // If no container ID, use internal state
                isActive = entry.getValue();
            }

            brokerInfoList.add(new BrokerInfo(brokerId, address, isActive, failureTime));
        }
        return brokerInfoList;
    }

    /**
     * Broker information class
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

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
 * Docker container management class for operating Kafka containers
 */
public class DockerManager {
    private DockerClient dockerClient;
    private static DockerManager instance;

    private DockerManager() {
        try {
            // Create Docker client configuration
            DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder().build();

            // Create HTTP client
            DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                    .dockerHost(config.getDockerHost())
                    .sslConfig(config.getSSLConfig())
                    .maxConnections(100)
                    .connectionTimeout(java.time.Duration.ofSeconds(30))
                    .responseTimeout(java.time.Duration.ofSeconds(45))
                    .build();

            // Create Docker client
            dockerClient = DockerClientBuilder.getInstance(config)
                    .withDockerHttpClient(httpClient)
                    .build();

            log("Docker client initialized successfully");
        } catch (Exception e) {
            log("Docker client initialization failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Get DockerManager singleton
     * @return DockerManager instance
     */
    public static synchronized DockerManager getInstance() {
        if (instance == null) {
            instance = new DockerManager();
        }
        return instance;
    }

    /**
     * Get all Kafka containers
     * @return List of Kafka containers
     */
    public List<Container> getKafkaContainers() {
        try {
            // List all containers with name containing "kafka"
            return dockerClient.listContainersCmd()
                    .withShowAll(true)
                    .withNameFilter(List.of("kafka"))
                    .exec();
        } catch (Exception e) {
            log("Failed to get Kafka containers: " + e.getMessage());
            e.printStackTrace();
            return List.of();
        }
    }

    /**
     * Get container by container ID
     * @param containerId Container ID
     * @return Container object
     */
    public Container getContainerById(String containerId) {
        try {
            List<Container> containers = dockerClient.listContainersCmd()
                    .withShowAll(true)
                    .withIdFilter(List.of(containerId))
                    .exec();
            return containers.isEmpty() ? null : containers.get(0);
        } catch (Exception e) {
            log("Failed to get container: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Get container by container name
     * @param containerName Container name
     * @return Container object
     */
    public Container getContainerByName(String containerName) {
        try {
            List<Container> containers = dockerClient.listContainersCmd()
                    .withShowAll(true)
                    .withNameFilter(List.of(containerName))
                    .exec();
            return containers.isEmpty() ? null : containers.get(0);
        } catch (Exception e) {
            log("Failed to get container: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Stop container
     * @param containerId Container ID
     * @return Whether the operation was successful
     */
    public boolean stopContainer(String containerId) {
        try {
            dockerClient.stopContainerCmd(containerId).exec();
            log("Stopped container: " + containerId);
            return true;
        } catch (Exception e) {
            log("Failed to stop container: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Start container
     * @param containerId Container ID
     * @return Whether the operation was successful
     */
    public boolean startContainer(String containerId) {
        try {
            dockerClient.startContainerCmd(containerId).exec();
            log("Started container: " + containerId);
            return true;
        } catch (Exception e) {
            log("Failed to start container: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Check if container is running
     * @param containerId Container ID
     * @return Whether the container is running
     */
    public boolean isContainerRunning(String containerId) {
        try {
            Container container = getContainerById(containerId);
            return container != null && container.getState().equals("running");
        } catch (Exception e) {
            log("Failed to check container status: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Close Docker client
     */
    public void close() {
        if (dockerClient != null) {
            try {
                dockerClient.close();
                log("Docker client has been closed");
            } catch (Exception e) {
                log("Failed to close Docker client: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}

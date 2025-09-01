package com.nan.kafkasimulator.controller;

import static com.nan.kafkasimulator.utils.Logger.log;

import java.net.URL;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import com.nan.kafkasimulator.ControllerRegistry;
import com.nan.kafkasimulator.utils.Alerter;

import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;

public class ConnectionManagerController implements Initializable {

    @FXML
    private TextField bootstrapServersField;

    @FXML
    private Button connectButton;
    @FXML
    private Button disconnectButton;

    private AdminClient adminClient;

    private String bootstrapServers;
    private Consumer<Boolean> onConnectionStateChanged;

    @Override
    public void initialize(URL arg0, ResourceBundle arg1) {
        bootstrapServersField.setText("localhost:19092");
        bootstrapServers = bootstrapServersField.getText();
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setOnConnectionStateChanged(Consumer<Boolean> onConnectionStateChanged) {
        this.onConnectionStateChanged = onConnectionStateChanged;
    }

    public void setStatusConnected(boolean connected) {
        connectButton.setDisable(connected);
        disconnectButton.setDisable(!connected);
    }

    @FXML
    protected void onConnectButtonClick() {
        connect();
    }

    private void displayClusterMetadata() throws ExecutionException, InterruptedException {
        if (adminClient == null)
            return;
        org.apache.kafka.clients.admin.DescribeClusterResult describeClusterResult = adminClient.describeCluster();
        java.util.Collection<org.apache.kafka.common.Node> nodes = describeClusterResult.nodes().get();

        log("\n--- Broker Information ---");
        for (org.apache.kafka.common.Node node : nodes) {
            log("Broker ID: " + node.id() + ", Address: " + node.host() + ":" + node.port());
        }
    }

    @FXML
    protected void onDisconnectButtonClick() {
        ControllerRegistry.getProducerController().cleanup();
        ControllerRegistry.getConsumerController().cleanup();
        disconnect();
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public void connect() {
        bootstrapServers = bootstrapServersField.getText();
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            Alerter.showAlert("Connection Error", null, "Please enter Kafka cluster address.");
            return;
        }

        log("Trying to connect to Kafka cluster: " + bootstrapServers);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "KafkaSimulator-AdminClient");

        Task<Void> connectTask = new Task<>() {
            @Override
            protected Void call() throws Exception {
                if (adminClient != null) {
                    adminClient.close();
                }
                adminClient = AdminClient.create(props);
                adminClient.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
                return null;
            }
        };

        connectTask.setOnSucceeded(event -> {
            javafx.application.Platform.runLater(() -> {
                log("Successfully connected to Kafka cluster!");
                try {

                    displayClusterMetadata();
                    onConnectionStateChanged.accept(true);
                } catch (ExecutionException | InterruptedException e) {
                    log("Failed to get cluster metadata: " + e.getMessage());
                    onConnectionStateChanged.accept(false);
                }
            });
        });

        connectTask.setOnFailed(event -> {
            javafx.application.Platform.runLater(() -> {
                Throwable e = connectTask.getException();
                log("Connection failed: " + e.getMessage());
                onConnectionStateChanged.accept(false);
            });
        });

        new Thread(connectTask).start();
    }

    public void disconnect() {
        if (adminClient != null) {
            log("Disconnecting from Kafka cluster...");
            ControllerRegistry.getProducerController().closeAllProducers();
            if (adminClient != null) {
                adminClient.close(java.time.Duration.ofSeconds(5));
                adminClient = null;
            }

            log("Successfully disconnected.");
            onConnectionStateChanged.accept(false);
        } else {
            log("Currently not connected to Kafka cluster.");
        }
    }

}

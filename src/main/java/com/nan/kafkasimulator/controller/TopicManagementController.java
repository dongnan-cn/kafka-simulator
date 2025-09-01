package com.nan.kafkasimulator.controller;

import javafx.fxml.FXML;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ListView;
import javafx.scene.control.TextField;

import static com.nan.kafkasimulator.utils.Logger.log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import com.nan.kafkasimulator.utils.Logger;

public class TopicManagementController {

    @FXML
    private ListView<String> topicsListView;

    public ListView<String> getTopicsListView() {
        return topicsListView;
    }

    @FXML
    private Button onRefreshTopicsButtonClick;
    @FXML
    private TextField newTopicNameField;
    @FXML
    private TextField numPartitionsField;
    @FXML
    private TextField replicationFactorField;
    @FXML
    private Button onCreateTopicButtonClick;
    @FXML
    private Button onDeleteTopicButtonClick;

    private AdminClient adminClient;
    private Consumer<List<String>> onTopicsUpdated;

    public void setAllControlsDisable(boolean disable) {
        newTopicNameField.setDisable(disable);
        numPartitionsField.setDisable(disable);
        replicationFactorField.setDisable(disable);
        onCreateTopicButtonClick.setDisable(disable);
        onRefreshTopicsButtonClick.setDisable(disable);
        onDeleteTopicButtonClick.setDisable(disable);
        topicsListView.setDisable(disable);
    }

    public void setAdminClient(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    public void setOnTopicsUpdated(Consumer<List<String>> onTopicsUpdated) {
        this.onTopicsUpdated = onTopicsUpdated;
    }

    @FXML
    protected void onRefreshTopicsButtonClick() {
        refreshTopicsList();

    }

    @FXML
    protected void onCreateTopicButtonClick() {
        try {
            int numPartitions = Integer.parseInt(numPartitionsField.getText());
            short replicationFactor = Short.parseShort(replicationFactorField.getText());
            createTopic(newTopicNameField.getText(), numPartitions, replicationFactor);
        } catch (NumberFormatException e) {
            Logger.log("Error: Number of partitions and replication factor must be valid numbers.");
        }

    }

    public void createTopic(String topicName, int numPartitions, short replicationFactor) {
        if (adminClient == null) {
            log("Error: Please connect to Kafka cluster first.");
            return;
        }

        if (topicName == null || topicName.trim().isEmpty()) {
            log("Error: Topic name cannot be empty.");
            return;
        }

        try {
            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            log("Successfully created Topic: " + topicName);
            refreshTopicsList();
        } catch (NumberFormatException e) {
            log("Error: Number of partitions and replication factor must be valid numbers.");
            showAlert("Input Error", null, "Number of partitions and replication factor must be valid numbers.");
        } catch (ExecutionException | InterruptedException e) {
            log("Failed to create Topic: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
        }
    }

    private void showAlert(String title, String header, String content) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle(title);
        alert.setHeaderText(header);
        alert.setContentText(content);
        alert.showAndWait();
    }

    @FXML
    protected void onDeleteTopicButtonClick() {
        deleteTopic();
    }

    public void refreshTopicsList() {
        if (adminClient == null) {
            log("Error: Please connect to Kafka cluster first.");
            return;
        }

        log("Refreshing Topic list...");
        try {
            Set<String> topicNames = adminClient.listTopics().names().get();

            // Platform.runLater(() -> {
            topicsListView.getItems().clear();
            topicsListView.getItems().addAll(topicNames);
            onTopicsUpdated.accept(new ArrayList<>(topicNames));
            log("Topic list refreshed successfully.");
            // });
        } catch (ExecutionException | InterruptedException e) {
            log("Failed to refresh Topic list: " + e.getMessage());
        }
    }

    public void deleteTopic() {
        if (adminClient == null) {
            log("Error: Please connect to Kafka cluster first.");
            return;
        }

        String selectedTopic = topicsListView.getSelectionModel().getSelectedItem();
        if (selectedTopic == null || selectedTopic.trim().isEmpty()) {
            log("Error: Please select a Topic to delete.");
            return;
        }

        try {
            adminClient.deleteTopics(Collections.singleton(selectedTopic)).all().get();
            log("Successfully deleted Topic: " + selectedTopic);
            refreshTopicsList();
        } catch (ExecutionException | InterruptedException e) {
            log("Failed to delete Topic: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
        }
    }

}

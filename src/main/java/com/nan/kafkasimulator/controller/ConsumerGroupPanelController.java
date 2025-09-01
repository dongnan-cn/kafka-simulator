package com.nan.kafkasimulator.controller;

import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.layout.VBox;

import org.apache.kafka.clients.admin.AdminClient;

import com.nan.kafkasimulator.ConsumerGroupManager;

import java.io.IOException;
import java.net.URL;
import java.util.ResourceBundle;

import static com.nan.kafkasimulator.utils.Logger.log;

/**
 * Consumer group panel controller, responsible for managing UI and interaction of a single consumer group
 */
public class ConsumerGroupPanelController implements Initializable {
    @FXML
    private VBox consumerGroupPanel;
    @FXML
    private Label consumerGroupIdLabel;
    @FXML
    private Button showPartitionAssignmentButton;
    @FXML
    private VBox messageArea;
    @FXML
    private TextArea messageTextArea;

    @FXML
    private Button addConsumerButton;
    @FXML
    private Button manualCommitButton;
    @FXML
    private Button stopConsumerGroupButton;
    @FXML
    private Button resumeConsumerGroupButton;

    private ConsumerGroupManager consumerGroupManager;
    private AdminClient adminClient;

    public ConsumerGroupPanelController() {
    }

    /**
     * Create a new consumer group panel
     * @param groupId Consumer group ID
     * @param topics List of subscribed topics
     * @param autoCommit Whether to auto commit offsets
     * @param autoCommitInterval Auto commit interval (milliseconds)
     * @param bootstrapServers Kafka server address
     * @param adminClient Kafka AdminClient
     * @return Consumer group panel controller
     */
    public static ConsumerGroupPanelController create(String groupId, java.util.List<String> topics,
            boolean autoCommit, String autoCommitInterval, String bootstrapServers, AdminClient adminClient) {
        try {
            FXMLLoader loader = new FXMLLoader(ConsumerGroupPanelController.class.getResource(
                    "/com/nan/kafkasimulator/fxml/consumer-group-panel.fxml"));
            loader.load();
            ConsumerGroupPanelController controller = loader.getController();

            // Initialize consumer group manager
            controller.consumerGroupManager = new ConsumerGroupManager(
                    groupId, topics, autoCommit, autoCommitInterval, bootstrapServers,
                    controller.messageTextArea);

            // Set consumer group ID
            controller.consumerGroupIdLabel.setText(groupId);

            // Set AdminClient
            controller.adminClient = adminClient;

            // Control visibility of manual commit button based on autoCommit parameter
            controller.manualCommitButton.setVisible(!autoCommit);

            // Start consumer group
            controller.consumerGroupManager.start(1);

            return controller;
        } catch (IOException e) {
            log("Failed to create consumer group panel: " + e.getMessage());
            return null;
        }
    }

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        // Bind button events
        stopConsumerGroupButton.setOnAction(event -> {
            consumerGroupManager.pauseAll();
            stopConsumerGroupButton.setDisable(true);
            resumeConsumerGroupButton.setDisable(false);
        });

        resumeConsumerGroupButton.setOnAction(event -> {
            consumerGroupManager.resumeAll();
            resumeConsumerGroupButton.setDisable(true);
            stopConsumerGroupButton.setDisable(false);
        });

        addConsumerButton.setOnAction(event -> consumerGroupManager.startNewConsumerInstance());

        manualCommitButton.setOnAction(event -> {
            consumerGroupManager.manualCommitAll();
        });

        showPartitionAssignmentButton.setOnAction(event -> {
            if (adminClient != null) {
                showPartitionAssignmentDialog();
            }
        });
    }

    /**
     * Show partition assignment dialog
     */
    private void showPartitionAssignmentDialog() {
        try {
            // Load FXML for partition assignment dialog
            URL fxmlUrl = getClass().getResource("/com/nan/kafkasimulator/fxml/partition-assignment-dialog.fxml");
            FXMLLoader loader = new FXMLLoader(fxmlUrl);
            DialogPane dialogPane = loader.load();
            // Get controller
            PartitionAssignmentDialogController controller = loader.getController();

            // Set consumer group ID
            controller.setConsumerGroupId(consumerGroupIdLabel.getText());

            // Create dialog
            Dialog<ButtonType> dialog = new Dialog<>();
            dialog.setDialogPane(dialogPane);
            dialog.setTitle("Partition Assignment Information");

            // Get partition assignment information immediately after dialog is shown
            dialog.setOnShown(event -> {
                // Get and display partition assignment information
                consumerGroupManager.showPartitionAssignments(adminClient, controller.getPartitionAssignmentTextArea());
            });

            // Show dialog
            dialog.showAndWait();
        } catch (IOException e) {
            e.printStackTrace();
            log("Failed to show partition assignment dialog: " + e.getMessage());
        }
    }

    /**
     * Get consumer group panel
     * @return Consumer group panel
     */
    public VBox getPanel() {
        return consumerGroupPanel;
    }

    /**
     * Get consumer group ID
     * @return Consumer group ID
     */
    public String getGroupId() {
        return consumerGroupManager.getGroupId();
    }

    /**
     * Get consumer group manager
     * @return Consumer group manager
     */
    public ConsumerGroupManager getConsumerGroupManager() {
        return consumerGroupManager;
    }

    /**
     * Stop consumer group
     */
    public void stop() {
        consumerGroupManager.stopAll();
    }

    /**
     * Set AdminClient
     * @param adminClient Kafka AdminClient
     */
    public void setAdminClient(AdminClient adminClient) {
        this.adminClient = adminClient;
    }
}
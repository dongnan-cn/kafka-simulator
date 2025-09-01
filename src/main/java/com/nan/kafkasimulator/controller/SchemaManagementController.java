package com.nan.kafkasimulator.controller;

import com.nan.kafkasimulator.avro.SchemaManager;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.control.Alert.AlertType;
import javafx.stage.Stage;

import java.net.URL;
import java.util.ResourceBundle;

import static com.nan.kafkasimulator.utils.Logger.log;

/**
 * Schema Management Dialog Controller
 */
public class SchemaManagementController implements Initializable {

    @FXML
    private ListView<String> schemaListView;

    @FXML
    private TextField schemaNameField;

    @FXML
    private TextArea schemaContentArea;

    @FXML
    private Button saveSchemaButton;

    @FXML
    private Button deleteSchemaButton;

    @FXML
    private Button validateSchemaButton;

    @FXML
    private Label validationResultLabel;

    @FXML
    private Button closeButton;

    private SchemaManager schemaManager;
    private ObservableList<String> schemaNames;
    private boolean isNewSchema = true;

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        schemaManager = SchemaManager.getInstance();
        schemaNames = FXCollections.observableArrayList();

        // 初始化Schema列表
        refreshSchemaList();

        // 设置列表选择监听器
        schemaListView.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal != null) {
                loadSchema(newVal);
                deleteSchemaButton.setDisable(false);
            } else {
                deleteSchemaButton.setDisable(true);
            }
        });

        // 设置Schema名称字段变化监听器
        schemaNameField.textProperty().addListener((obs, oldVal, newVal) -> {
            isNewSchema = !schemaNames.contains(newVal);
            updateButtonStates();
        });
    }

    /**
     * Refresh Schema list
     */
    private void refreshSchemaList() {
        schemaNames.clear();
        schemaNames.addAll(schemaManager.getAllSchemas().keySet());
        schemaListView.setItems(schemaNames);
    }

    /**
     * Load selected Schema
     * @param schemaName Schema name
     */
    private void loadSchema(String schemaName) {
        SchemaManager.SchemaVersion version = schemaManager.getSchemaVersion(schemaName);

        if (version != null) {
            schemaNameField.setText(schemaName);
            schemaContentArea.setText(version.getSchemaJson());
            isNewSchema = false;
            updateButtonStates();
        }
    }

    /**
     * Update button states
     */
    private void updateButtonStates() {
        if (isNewSchema) {
            saveSchemaButton.setText("Add Schema");
            deleteSchemaButton.setDisable(true);
        } else {
            saveSchemaButton.setText("Update Schema");
            deleteSchemaButton.setDisable(false);
        }

        // 验证按钮始终可用
        validateSchemaButton.setDisable(false);
    }

    /**
     * Clear form
     */
    private void clearForm() {
        schemaNameField.clear();
        schemaContentArea.clear();
        validationResultLabel.setText("");
        isNewSchema = true;
        schemaListView.getSelectionModel().clearSelection();
        updateButtonStates();
    }

    // FXML事件处理方法
    @FXML
    private void onAddSchemaButtonClick() {
        clearForm();
        schemaNameField.requestFocus();
    }

    @FXML
    private void onDeleteSchemaButtonClick() {
        String schemaName = schemaNameField.getText();
        if (schemaName == null || schemaName.trim().isEmpty()) {
            showAlert(AlertType.WARNING, "Warning", "Please select a Schema to delete");
            return;
        }

        Alert alert = new Alert(AlertType.CONFIRMATION);
        alert.setTitle("Confirm Delete");
        alert.setHeaderText("Are you sure you want to delete Schema '" + schemaName + "'?");
        alert.showAndWait().ifPresent(response -> {
            if (response == ButtonType.OK) {
                try {
                    schemaManager.removeSchema(schemaName);
                    log("Successfully deleted Schema: " + schemaName);
                    refreshSchemaList();
                    clearForm();
                    showAlert(AlertType.INFORMATION, "Success", "Schema has been successfully deleted");
                } catch (Exception e) {
                    log("Failed to delete Schema: " + e.getMessage());
                    showAlert(AlertType.ERROR, "Error", "Failed to delete Schema: " + e.getMessage());
                }
            }
        });
    }

    @FXML
    private void onSaveSchemaButtonClick() {
        String schemaName = schemaNameField.getText();
        String schemaContent = schemaContentArea.getText();

        if (schemaName == null || schemaName.trim().isEmpty()) {
            showAlert(AlertType.WARNING, "Warning", "Schema name cannot be empty");
            return;
        }

        if (schemaContent == null || schemaContent.trim().isEmpty()) {
            showAlert(AlertType.WARNING, "Warning", "Schema content cannot be empty");
            return;
        }

        try {
            if (isNewSchema) {
                // Add new Schema
                schemaManager.registerSchema(schemaName, schemaContent);
                log("Successfully added Schema: " + schemaName);
                showAlert(AlertType.INFORMATION, "Success", "Schema has been successfully added");
            } else {
                // Update existing Schema
                schemaManager.updateSchema(schemaName, schemaContent);
                log("Successfully updated Schema: " + schemaName);
                showAlert(AlertType.INFORMATION, "Success", "Schema has been successfully updated");
            }

            refreshSchemaList();
            schemaListView.getSelectionModel().select(schemaName);
        } catch (Exception e) {
            log("Failed to save Schema: " + e.getMessage());
            showAlert(AlertType.ERROR, "Error", "Failed to save Schema: " + e.getMessage());
        }
    }

    @FXML
    private void onValidateSchemaButtonClick() {
        String schemaContent = schemaContentArea.getText();

        if (schemaContent == null || schemaContent.trim().isEmpty()) {
            validationResultLabel.setText("Please enter Schema content");
            validationResultLabel.setStyle("-fx-text-fill: red;");
            return;
        }

        if (schemaManager.validateSchema(schemaContent)) {
            validationResultLabel.setText("Schema format is valid");
            validationResultLabel.setStyle("-fx-text-fill: green;");
        } else {
            validationResultLabel.setText("Schema format is invalid");
            validationResultLabel.setStyle("-fx-text-fill: red;");
        }
    }

    @FXML
    private void onCloseButtonClick() {
        Stage stage = (Stage) closeButton.getScene().getWindow();
        stage.close();
    }

    /**
     * 显示提示对话框
     * @param type 对话框类型
     * @param title 标题
     * @param message 消息内容
     */
    private void showAlert(AlertType type, String title, String message) {
        Alert alert = new Alert(type);
        alert.setTitle(title);
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.showAndWait();
    }
}

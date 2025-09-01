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
 * Schema管理对话框控制器
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
     * 刷新Schema列表
     */
    private void refreshSchemaList() {
        schemaNames.clear();
        schemaNames.addAll(schemaManager.getAllSchemas().keySet());
        schemaListView.setItems(schemaNames);
    }

    /**
     * 加载选定的Schema
     * @param schemaName Schema名称
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
     * 更新按钮状态
     */
    private void updateButtonStates() {
        if (isNewSchema) {
            saveSchemaButton.setText("添加Schema");
            deleteSchemaButton.setDisable(true);
        } else {
            saveSchemaButton.setText("更新Schema");
            deleteSchemaButton.setDisable(false);
        }

        // 验证按钮始终可用
        validateSchemaButton.setDisable(false);
    }

    /**
     * 清空表单
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
            showAlert(AlertType.WARNING, "警告", "请选择要删除的Schema");
            return;
        }

        Alert alert = new Alert(AlertType.CONFIRMATION);
        alert.setTitle("确认删除");
        alert.setHeaderText("确定要删除Schema '" + schemaName + "' 吗？");
        alert.showAndWait().ifPresent(response -> {
            if (response == ButtonType.OK) {
                try {
                    schemaManager.removeSchema(schemaName);
                    log("成功删除Schema: " + schemaName);
                    refreshSchemaList();
                    clearForm();
                    showAlert(AlertType.INFORMATION, "成功", "Schema已成功删除");
                } catch (Exception e) {
                    log("删除Schema失败: " + e.getMessage());
                    showAlert(AlertType.ERROR, "错误", "删除Schema失败: " + e.getMessage());
                }
            }
        });
    }

    @FXML
    private void onSaveSchemaButtonClick() {
        String schemaName = schemaNameField.getText();
        String schemaContent = schemaContentArea.getText();

        if (schemaName == null || schemaName.trim().isEmpty()) {
            showAlert(AlertType.WARNING, "警告", "Schema名称不能为空");
            return;
        }

        if (schemaContent == null || schemaContent.trim().isEmpty()) {
            showAlert(AlertType.WARNING, "警告", "Schema内容不能为空");
            return;
        }

        try {
            if (isNewSchema) {
                // 添加新Schema
                schemaManager.registerSchema(schemaName, schemaContent);
                log("成功添加Schema: " + schemaName);
                showAlert(AlertType.INFORMATION, "成功", "Schema已成功添加");
            } else {
                // 更新现有Schema
                schemaManager.updateSchema(schemaName, schemaContent);
                log("成功更新Schema: " + schemaName);
                showAlert(AlertType.INFORMATION, "成功", "Schema已成功更新");
            }

            refreshSchemaList();
            schemaListView.getSelectionModel().select(schemaName);
        } catch (Exception e) {
            log("保存Schema失败: " + e.getMessage());
            showAlert(AlertType.ERROR, "错误", "保存Schema失败: " + e.getMessage());
        }
    }

    @FXML
    private void onValidateSchemaButtonClick() {
        String schemaContent = schemaContentArea.getText();

        if (schemaContent == null || schemaContent.trim().isEmpty()) {
            validationResultLabel.setText("请输入Schema内容");
            validationResultLabel.setStyle("-fx-text-fill: red;");
            return;
        }

        if (schemaManager.validateSchema(schemaContent)) {
            validationResultLabel.setText("Schema格式有效");
            validationResultLabel.setStyle("-fx-text-fill: green;");
        } else {
            validationResultLabel.setText("Schema格式无效");
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

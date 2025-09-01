package com.nan.kafkasimulator.controller;

import com.nan.kafkasimulator.BrokerFailureSimulator;
import com.nan.kafkasimulator.ControllerRegistry;
import com.nan.kafkasimulator.DockerManager;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.HBox;

import java.net.URL;
import java.util.ResourceBundle;

import static com.nan.kafkasimulator.utils.Logger.log;

/**
 * Broker Failure Simulation Controller
 */
public class BrokerFailureController implements Initializable {

    @FXML
    private TableView<BrokerFailureSimulator.BrokerInfo> brokerTableView;

    @FXML
    private TableColumn<BrokerFailureSimulator.BrokerInfo, Integer> idColumn;

    @FXML
    private TableColumn<BrokerFailureSimulator.BrokerInfo, String> addressColumn;

    @FXML
    private TableColumn<BrokerFailureSimulator.BrokerInfo, Boolean> statusColumn;

    @FXML
    private TableColumn<BrokerFailureSimulator.BrokerInfo, Long> failureTimeColumn;

    @FXML
    private TableColumn<BrokerFailureSimulator.BrokerInfo, Void> actionsColumn;

    @FXML
    private Label statusLabel;

    @FXML
    private Button refreshButton;

    @FXML
    private Button failRandomBrokerButton;

    @FXML
    private Button recoverAllBrokersButton;

    private BrokerFailureSimulator brokerFailureSimulator;

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        // 初始化表格列
        idColumn.setCellValueFactory(new PropertyValueFactory<>("id"));
        addressColumn.setCellValueFactory(new PropertyValueFactory<>("address"));
        statusColumn.setCellValueFactory(new PropertyValueFactory<>("active"));
        failureTimeColumn.setCellValueFactory(new PropertyValueFactory<>("failureTime"));

        // 设置状态列的单元格工厂，以显示更有意义的状态文本
        statusColumn.setCellFactory(column -> new TableCell<BrokerFailureSimulator.BrokerInfo, Boolean>() {
            @Override
            protected void updateItem(Boolean item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || getTableRow() == null || getTableRow().getItem() == null) {
                    setText(null);
                    setGraphic(null);
                } else {
                    BrokerFailureSimulator.BrokerInfo brokerInfo = getTableRow().getItem();
                    boolean isActive = brokerInfo.isActive();
                    setText(isActive ? "Normal" : "Down");
                    setTextFill(isActive ? javafx.scene.paint.Color.GREEN : javafx.scene.paint.Color.RED);
                }
            }
        });

        // 设置故障时间列的单元格工厂，以格式化显示时间
        failureTimeColumn.setCellFactory(column -> new TableCell<BrokerFailureSimulator.BrokerInfo, Long>() {
            @Override
            protected void updateItem(Long item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || getTableRow() == null || getTableRow().getItem() == null) {
                    setText(null);
                } else {
                    BrokerFailureSimulator.BrokerInfo brokerInfo = getTableRow().getItem();
                    Long failureTime = brokerInfo.getFailureTime();
                    if (failureTime != null) {
                        setText(new java.util.Date(failureTime).toString());
                    } else {
                        setText("N/A");
                    }
                }
            }
        });

        // 设置操作列的单元格工厂，以添加操作按钮
        actionsColumn.setCellFactory(param -> new TableCell<BrokerFailureSimulator.BrokerInfo, Void>() {
            private final Button failButton = new Button("Fail");
            private final Button recoverButton = new Button("Recover");
            private final HBox pane = new HBox(5, failButton, recoverButton);

            {
                failButton.setOnAction(event -> {
                    BrokerFailureSimulator.BrokerInfo brokerInfo = getTableView().getItems().get(getIndex());
                    handleFailBroker(brokerInfo.getId());
                });

                recoverButton.setOnAction(event -> {
                    BrokerFailureSimulator.BrokerInfo brokerInfo = getTableView().getItems().get(getIndex());
                    handleRecoverBroker(brokerInfo.getId());
                });
            }

            @Override
            protected void updateItem(Void item, boolean empty) {
                super.updateItem(item, empty);
                if (empty) {
                    setGraphic(null);
                } else {
                    BrokerFailureSimulator.BrokerInfo brokerInfo = getTableView().getItems().get(getIndex());
                    boolean isActive = brokerInfo.isActive();

                    // 根据broker状态启用/禁用按钮
                    failButton.setDisable(!isActive);
                    recoverButton.setDisable(isActive);

                    setGraphic(pane);
                }
            }
        });

        // 初始化按钮事件
        refreshButton.setOnAction(event -> refreshBrokerList()); // 手动刷新Broker列表
        failRandomBrokerButton.setOnAction(event -> handleFailRandomBroker());
        recoverAllBrokersButton.setOnAction(event -> handleRecoverAllBrokers());

        // 初始化状态
        setControlsDisable(true);
        statusLabel.setText("Not connected to Kafka cluster");
    }

    /**
     * 设置控制器是否可用
     * @param connected 是否已连接到Kafka集群
     */
    public void setConnected(boolean connected) {
        Platform.runLater(() -> {
            setControlsDisable(!connected);
            if (connected) {
                statusLabel.setText("Connected to Kafka cluster");
                initializeBrokerFailureSimulator();
                startPeriodicUpdate();
            } else {
                statusLabel.setText("Not connected to Kafka cluster");
                stopPeriodicUpdate();
                brokerFailureSimulator = null;
                brokerTableView.getItems().clear();
            }
        });
    }

    /**
     * 设置控件是否禁用
     * @param disable 是否禁用
     */
    private void setControlsDisable(boolean disable) {
        refreshButton.setDisable(disable);
        failRandomBrokerButton.setDisable(disable);
        recoverAllBrokersButton.setDisable(disable);
        brokerTableView.setDisable(disable);
    }

    /**
     * 初始化Broker故障模拟器
     */
    private void initializeBrokerFailureSimulator() {
        if (brokerFailureSimulator == null) {
            // 初始化DockerManager
            DockerManager.getInstance();

            brokerFailureSimulator = new BrokerFailureSimulator(
                    ControllerRegistry.getConnectionManagerController().getAdminClient());
            refreshBrokerList();
        }
    }

    /**
     * 开始定期更新（已禁用）
     */
    private void startPeriodicUpdate() {
        // 不再需要定时刷新，只在手动操作时刷新
        // updateTimer = null;
    }

    /**
     * 停止定期更新（已禁用）
     */
    private void stopPeriodicUpdate() {
        // 不再需要定时刷新，只在手动操作时刷新
        // updateTimer = null;
    }

    /**
     * 刷新broker列表
     */
    private void refreshBrokerList() {
        if (brokerFailureSimulator == null) {
            return;
        }

        try {
            brokerTableView.getItems().clear();
            brokerTableView.getItems().addAll(brokerFailureSimulator.getAllBrokerInfo());
            log("Broker list has been refreshed");
        } catch (Exception e) {
            log("Failed to refresh broker list: " + e.getMessage());
        }
    }

    /**
     * 处理宕机单个broker
     * @param brokerId broker ID
     */
    private void handleFailBroker(int brokerId) {
        if (brokerFailureSimulator == null) {
            return;
        }

        try {
            boolean success = brokerFailureSimulator.failBroker(brokerId);
            if (success) {
                log("Simulated Broker " + brokerId + " failure");
                refreshBrokerList();
            }
        } catch (Exception e) {
            log("Failed to simulate Broker " + brokerId + " failure: " + e.getMessage());
        }
    }

    /**
     * 处理恢复单个broker
     * @param brokerId broker ID
     */
    private void handleRecoverBroker(int brokerId) {
        if (brokerFailureSimulator == null) {
            return;
        }

        try {
            boolean success = brokerFailureSimulator.recoverBroker(brokerId);
            if (success) {
                log("Simulated Broker " + brokerId + " recovery");
                refreshBrokerList();
            }
        } catch (Exception e) {
            log("Failed to simulate Broker " + brokerId + " recovery: " + e.getMessage());
        }
    }

    /**
     * 处理随机宕机一个broker
     */
    private void handleFailRandomBroker() {
        if (brokerFailureSimulator == null) {
            return;
        }

        try {
            // 获取所有正常状态的broker
            java.util.List<BrokerFailureSimulator.BrokerInfo> activeBrokers = new java.util.ArrayList<>();
            for (BrokerFailureSimulator.BrokerInfo brokerInfo : brokerFailureSimulator.getAllBrokerInfo()) {
                if (brokerInfo.isActive()) {
                    activeBrokers.add(brokerInfo);
                }
            }

            if (activeBrokers.isEmpty()) {
                log("No available Brokers to fail");
                return;
            }

            // 随机选择一个broker
            java.util.Random random = new java.util.Random();
            BrokerFailureSimulator.BrokerInfo randomBroker = activeBrokers.get(random.nextInt(activeBrokers.size()));

            // 宕机选中的broker
            handleFailBroker(randomBroker.getId());
        } catch (Exception e) {
            log("Random Broker failure failed: " + e.getMessage());
        }
    }

    /**
     * 处理恢复所有broker
     */
    private void handleRecoverAllBrokers() {
        if (brokerFailureSimulator == null) {
            return;
        }

        try {
            // 获取所有宕机状态的broker
            java.util.List<Integer> failedBrokerIds = new java.util.ArrayList<>();
            for (BrokerFailureSimulator.BrokerInfo brokerInfo : brokerFailureSimulator.getAllBrokerInfo()) {
                if (!brokerInfo.isActive()) {
                    failedBrokerIds.add(brokerInfo.getId());
                }
            }

            if (failedBrokerIds.isEmpty()) {
                log("No failed Brokers to recover");
                return;
            }

            // 恢复所有宕机的broker
            for (Integer brokerId : failedBrokerIds) {
                handleRecoverBroker(brokerId);
            }
        } catch (Exception e) {
            log("Failed to recover all Brokers: " + e.getMessage());
        }
    }

    /**
     * 清理资源
     */
    public void cleanup() {
        stopPeriodicUpdate();
        brokerFailureSimulator = null;
        // 关闭DockerManager
        DockerManager.getInstance().close();
    }
}

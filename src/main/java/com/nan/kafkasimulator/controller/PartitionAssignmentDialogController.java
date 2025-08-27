package com.nan.kafkasimulator.controller;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.application.Platform;

import org.apache.kafka.clients.admin.AdminClient;

import com.nan.kafkasimulator.ConsumerGroupManager;

import java.net.URL;
import java.util.ResourceBundle;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * 分区分配对话框控制器，负责管理分区分配对话框的UI和交互
 */
public class PartitionAssignmentDialogController implements Initializable {
    @FXML
    private Label consumerGroupIdLabel;
    @FXML
    private TextArea partitionAssignmentTextArea;
    @FXML
    private CheckBox autoRefreshCheckBox;
    @FXML
    private Button refreshButton;
    
    private ScheduledExecutorService scheduler;
    private ConsumerGroupManager consumerGroupManager;
    private AdminClient adminClient;

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        // 初始化代码
        autoRefreshCheckBox.setSelected(true); // 默认开启自动刷新
        
        // 自动刷新复选框事件
        autoRefreshCheckBox.setOnAction(event -> {
            if (autoRefreshCheckBox.isSelected()) {
                startAutoRefresh();
            } else {
                stopAutoRefresh();
            }
        });
        
        // 刷新按钮事件
        refreshButton.setOnAction(event -> refreshPartitionAssignment());
    }
    
    /**
     * 设置消费者组ID
     * @param groupId 消费者组ID
     */
    public void setConsumerGroupId(String groupId) {
        consumerGroupIdLabel.setText(groupId);
    }

    /**
     * 获取分区分配文本区域
     * @return 分区分配文本区域
     */
    public TextArea getPartitionAssignmentTextArea() {
        return partitionAssignmentTextArea;
    }
    
    /**
     * 设置消费者组管理器和AdminClient
     * @param consumerGroupManager 消费者组管理器
     * @param adminClient Kafka AdminClient
     */
    public void setConsumerGroupManager(ConsumerGroupManager consumerGroupManager, AdminClient adminClient) {
        this.consumerGroupManager = consumerGroupManager;
        this.adminClient = adminClient;
        
        // 初始刷新一次
        refreshPartitionAssignment();
        
        // 如果自动刷新开启，启动自动刷新
        if (autoRefreshCheckBox.isSelected()) {
            startAutoRefresh();
        }
    }
    
    /**
     * 开始自动刷新
     */
    private void startAutoRefresh() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
        
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            Platform.runLater(this::refreshPartitionAssignment);
        }, 2, 3, TimeUnit.SECONDS); // 初始延迟2秒，之后每3秒刷新一次
    }
    
    /**
     * 停止自动刷新
     */
    private void stopAutoRefresh() {
        if (scheduler != null) {
            scheduler.shutdown();
            scheduler = null;
        }
    }
    
    /**
     * 刷新分区分配信息
     */
    private void refreshPartitionAssignment() {
        if (consumerGroupManager != null && adminClient != null) {
            consumerGroupManager.showPartitionAssignments(adminClient, partitionAssignmentTextArea);
        }
    }
    
    /**
     * 清理资源
     */
    public void cleanup() {
        stopAutoRefresh();
    }
}

package com.nan.kafkasimulator.controller;

import com.nan.kafkasimulator.utils.Logger;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.TextArea;

import java.net.URL;
import java.util.ResourceBundle;

public class LogController implements Initializable {

    @FXML
    private TextArea logArea;

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        // 初始化Logger
        Logger.getInstance().initialize(logArea);
    }
}

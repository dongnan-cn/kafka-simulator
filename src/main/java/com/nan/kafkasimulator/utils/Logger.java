package com.nan.kafkasimulator.utils;

import javafx.application.Platform;
import javafx.scene.control.TextArea;

public class Logger {
    private static Logger instance;
    private static TextArea logArea;

    private Logger() {
    }

    public static Logger getInstance() {
        if (instance == null) {
            instance = new Logger();
        }
        return instance;
    }

    public void initialize(TextArea area) {
        if (logArea != null) {
            throw new IllegalStateException("Logger has already been initialized.");
        }
        logArea = area;
    }

    public static void log(String message) {
        if (logArea == null) {
            System.err.println("Logger is not initialized! Message: " + message);
            return;
        }
        Platform.runLater(() -> {
            logArea.appendText(message + "\n");
            logArea.setScrollTop(Double.MAX_VALUE);
        });
    }
}

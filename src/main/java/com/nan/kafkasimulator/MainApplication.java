package com.nan.kafkasimulator;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.LogManager;

public class MainApplication extends Application {
    static {
        try (InputStream is = MainApplication.class.getClassLoader().getResourceAsStream("logging.properties")) {
            if (is != null) {
                LogManager.getLogManager().readConfiguration(is);
            }
        } catch (IOException e) {
            System.err.println("Failed to load logging configuration: " + e.getMessage());
        }
    }

    @Override
    public void start(Stage stage) throws IOException {
        FXMLLoader fxmlLoader = new FXMLLoader(MainApplication.class.getResource("main-view.fxml"));
        Scene scene = new Scene(fxmlLoader.load(), 1200, 800);
        MainController controller = fxmlLoader.getController();
        stage.setTitle("Kafka Simulator");
        stage.setScene(scene);
        stage.setOnCloseRequest(event -> {
            System.out.println("Window closing, performing cleanup...");
            controller.cleanup();
            com.nan.kafkasimulator.utils.Logger.shutdown();
        });
        stage.show();
    }

    public static void main(String[] args) {
        launch();
    }
}

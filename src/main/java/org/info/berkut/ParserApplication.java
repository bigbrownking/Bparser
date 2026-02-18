package org.info.berkut;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import org.info.berkut.service.Parser;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.File;
import java.io.FileWriter;
import java.time.LocalDate;
import java.time.YearMonth;

public class ParserApplication extends Application {

    private ConfigurableApplicationContext springContext;
    private Parser parser;

    private ComboBox<String> monthComboBox;
    private ComboBox<Integer> yearComboBox;
    private Button exportButton;
    private Label statusLabel;
    private ProgressIndicator progressIndicator;

    private TextField startPageField;

    @Override
    public void init() {
        SpringApplication app = new SpringApplication(BerkutApplication.class);
        app.setHeadless(false);
        springContext = app.run();
        parser = springContext.getBean(Parser.class);
    }

    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("Berkut Data Parser");

        Label titleLabel = new Label("Export Crossing Facts");
        titleLabel.setStyle("-fx-font-size: 18px; -fx-font-weight: bold;");

        // MONTH
        Label monthLabel = new Label("Month:");
        monthComboBox = new ComboBox<>();
        monthComboBox.getItems().addAll(
                "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"
        );
        monthComboBox.setValue("January");
        monthComboBox.setPrefWidth(200);

        // YEAR
        Label yearLabel = new Label("Year:");
        yearComboBox = new ComboBox<>();
        int currentYear = LocalDate.now().getYear();
        for (int year = currentYear; year >= currentYear - 10; year--) {
            yearComboBox.getItems().add(year);
        }
        yearComboBox.setValue(currentYear);
        yearComboBox.setPrefWidth(200);

        // START PAGE
        Label startPageLabel = new Label("Start from page:");
        startPageField = new TextField("1");
        startPageField.setPrefWidth(200);
        startPageField.setPromptText("1");

        // BUTTON
        exportButton = new Button("Export CSV");
        exportButton.setPrefWidth(200);
        exportButton.setStyle("-fx-background-color: #4CAF50; -fx-text-fill: white; -fx-font-weight: bold;");
        exportButton.setOnAction(e -> handleExport());

        // PROGRESS
        progressIndicator = new ProgressIndicator();
        progressIndicator.setVisible(false);
        progressIndicator.setPrefSize(40, 40);

        // STATUS
        statusLabel = new Label("");
        statusLabel.setWrapText(true);
        statusLabel.setAlignment(Pos.CENTER);
        statusLabel.setPrefWidth(280);

        // LAYOUT
        VBox layout = new VBox(15);
        layout.setPadding(new Insets(20));
        layout.setAlignment(Pos.CENTER);
        layout.getChildren().addAll(
                titleLabel,
                monthLabel, monthComboBox,
                yearLabel, yearComboBox,
                startPageLabel, startPageField,
                exportButton,
                progressIndicator,
                statusLabel
        );

        Scene scene = new Scene(layout, 350, 450);
        primaryStage.setScene(scene);
        primaryStage.setResizable(false);
        primaryStage.setOnCloseRequest(e -> {
            Platform.exit();
            System.exit(0);
        });

        primaryStage.show();
    }

    private void handleExport() {

        int selectedMonth = monthComboBox.getSelectionModel().getSelectedIndex() + 1;
        int selectedYear = yearComboBox.getValue();

        int startPage;
        try {
            startPage = Integer.parseInt(startPageField.getText().trim());
            if (startPage < 1) startPage = 1;
        } catch (Exception ex) {
            showStatus("Invalid start page", "red");
            return;
        }

        YearMonth yearMonth = YearMonth.of(selectedYear, selectedMonth);
        String dateFrom = yearMonth.atDay(1) + "T00:00:00+05:00";
        String dateTo = yearMonth.atEndOfMonth() + "T23:59:59+05:00";

        // UI lock
        exportButton.setDisable(true);
        progressIndicator.setVisible(true);
        showStatus("Exporting data...", "blue");

        parser.exportCsv(dateFrom, dateTo, startPage)
                .subscribe(
                        csvData -> Platform.runLater(() -> {
                            if (csvData != null && !csvData.isEmpty()) {
                                saveFile(csvData, selectedMonth, selectedYear);
                            } else {
                                showStatus("No data found for selected period", "orange");
                            }
                            resetUI();
                        }),
                        ex -> Platform.runLater(() -> {
                            showStatus("Error: " + ex.getMessage(), "red");
                            showAlert("Export Failed",
                                    "An error occurred during export:\n" + ex.getMessage(),
                                    Alert.AlertType.ERROR);
                            resetUI();
                        })
                );
    }

    private void saveFile(String csvData, int month, int year) {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Save CSV File");
        fileChooser.setInitialFileName(String.format("crossing_facts_%d-%02d.csv", year, month));
        fileChooser.getExtensionFilters().add(
                new FileChooser.ExtensionFilter("CSV Files", "*.csv")
        );

        File file = fileChooser.showSaveDialog(exportButton.getScene().getWindow());

        if (file != null) {
            try (FileWriter writer = new FileWriter(file)) {
                writer.write(csvData);
                showStatus("File saved successfully!", "green");
                showAlert("Success",
                        "CSV file has been saved to:\n" + file.getAbsolutePath(),
                        Alert.AlertType.INFORMATION);
            } catch (Exception e) {
                showStatus("Failed to save file", "red");
                showAlert("Save Error",
                        "Could not save file:\n" + e.getMessage(),
                        Alert.AlertType.ERROR);
            }
        } else {
            showStatus("Export cancelled", "gray");
        }
    }

    private void showStatus(String message, String color) {
        statusLabel.setText(message);
        statusLabel.setStyle("-fx-text-fill: " + color + ";");
    }

    private void resetUI() {
        exportButton.setDisable(false);
        progressIndicator.setVisible(false);
    }

    private void showAlert(String title, String content, Alert.AlertType type) {
        Alert alert = new Alert(type);
        alert.setTitle(title);
        alert.setHeaderText(null);
        alert.setContentText(content);
        alert.showAndWait();
    }

    @Override
    public void stop() {
        if (springContext != null) {
            springContext.close();
        }
    }

    public static void main(String[] args) {
        launch(args);
    }
}

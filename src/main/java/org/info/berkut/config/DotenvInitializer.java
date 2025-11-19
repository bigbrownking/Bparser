package org.info.berkut.config;

import io.github.cdimascio.dotenv.Dotenv;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class DotenvInitializer implements EnvironmentPostProcessor {
    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        try {
            String workingDir = new File(".").getAbsolutePath();
            log.info("Current working directory: {}", workingDir);

            Dotenv dotenv = Dotenv.configure()
                    .directory(".")
                    .ignoreIfMissing()
                    .load();

            if (dotenv == null || dotenv.entries().isEmpty()) {
                File targetDir = new File(workingDir);
                File projectDir = targetDir.getParentFile();
                if (projectDir != null && projectDir.exists()) {
                    log.info("Attempting to load .env from project directory: {}", projectDir.getAbsolutePath());
                    dotenv = Dotenv.configure()
                            .directory(projectDir.getAbsolutePath())
                            .ignoreIfMissing()
                            .load();
                }
            }

            if (dotenv != null && !dotenv.entries().isEmpty()) {
                log.info(".env file loaded with {} entries from: {}", dotenv.entries().size(), new File(".").getAbsolutePath());
                Map<String, Object> dotenvMap = new HashMap<>();
                dotenv.entries().forEach(entry -> dotenvMap.put(entry.getKey(), entry.getValue()));

                environment.getPropertySources().addFirst(new MapPropertySource("dotenv", dotenvMap));
                log.info("Added .env properties to Spring environment.");
            } else {
                log.warn("No .env file found or it is empty in working directory or project root.");
            }
        } catch (Exception e) {
            log.error("Failed to load .env file: {}", e.getMessage());
            throw new RuntimeException("Failed to load .env file", e);
        }
    }
}

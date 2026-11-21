package com.formation.eda.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Kafka Streams : agregations fenetrees et detection d'anomalies.
 */
@SpringBootApplication
public class AnalyticsServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsServiceApplication.class, args);
    }
}

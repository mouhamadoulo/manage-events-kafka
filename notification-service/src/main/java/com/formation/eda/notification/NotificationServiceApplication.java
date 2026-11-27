package com.formation.eda.notification;

import com.formation.eda.kafka.reliability.ConsumerReliabilityConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

/**
 * Consumer terminal idempotent. Pur read-side.
 *
 * <p>L'{@code @Import} est la seule ligne qui active toute la mecanique de fiabilite de la
 * phase 4 : idempotence, retry etage, DLT. WHY explicite et non auto-configure : les 6 services
 * dependent de {@code common-kafka}, et une auto-configuration s'appliquerait aussi aux
 * consumers d'{@code order-service} et au binder Kafka Streams d'{@code analytics-service}.
 * Une ligne qu'on peut suivre vaut mieux qu'une magie a expliquer — et le profil
 * {@code cloud-stream} de ce service doit pouvoir rester nu pour servir de temoin.
 */
@SpringBootApplication
@Import(ConsumerReliabilityConfiguration.class)
public class NotificationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }
}

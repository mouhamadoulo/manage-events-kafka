package com.formation.eda.notification;

import com.formation.eda.kafka.reliability.ProcessedEventStore;
import com.formation.eda.kafka.reliability.RetryStageSupport;
import com.formation.eda.notification.application.FaultInjection;
import com.formation.eda.notification.application.OrderNotificationHandler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifie que le contexte Spring demarre, <b>et</b> que l'{@code @Import} de la phase 4 a bien
 * cable la mecanique de fiabilite. Un contexte qui demarre sans intercepteur d'idempotence
 * serait vert ici et perdrait des doublons en production.
 *
 * <p>Ni broker ni base requis : les producteurs Kafka se connectent paresseusement, Hikari
 * n'ouvre une connexion qu'a la premiere requete, et Flyway est desactive — c'est le seul
 * composant qui exigerait une base <b>au demarrage</b>. La table elle-meme est verifiee contre
 * un vrai Postgres par {@code JdbcProcessedEventStoreIT}, sous le profil {@code integration}.
 */
@SpringBootTest(properties = {
        "spring.flyway.enabled=false",
        "spring.kafka.listener.auto-startup=false"
})
class NotificationServiceApplicationTest {

    @Autowired
    private OrderNotificationHandler handler;

    @Autowired
    private ProcessedEventStore processedEventStore;

    @Autowired
    private RetryStageSupport retryStageSupport;

    @Autowired
    private DeadLetterPublishingRecoverer deadLetterPublishingRecoverer;

    @Autowired
    private FaultInjection faultInjection;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @org.springframework.beans.factory.annotation.Value("${server.address}")
    private String serverAddress;

    @Test
    @DisplayName("le contexte demarre avec la mecanique de fiabilite branchee")
    void contextLoadsWithReliabilityWiredIn() {
        assertThat(handler).isNotNull();
        assertThat(processedEventStore).isNotNull();
        assertThat(retryStageSupport).isNotNull();
        assertThat(deadLetterPublishingRecoverer).isNotNull();
    }

    @Test
    @DisplayName("l'auto-configuration Flyway de Boot est au classpath")
    void bootFlywayAutoConfigurationIsOnTheClasspath() {
        // PIEGE Spring Boot 4, trouve en P4-T6 : « org.flywaydb:flyway-core » apporte la
        // bibliotheque, PAS l'auto-configuration — elle vit dans le module spring-boot-flyway.
        // Avec flyway-core seul, le contexte demarre sans rien dire et aucune migration n'est
        // appliquee : la table processed_event n'existait pas, et chaque evenement partait en
        // DLT sur BadSqlGrammarException.
        //
        // Ce test ne peut pas verifier qu'une migration s'applique (il n'y a pas de base ici).
        // Il verifie la seule chose qui manquait : que le module d'auto-configuration est la.
        assertThat(org.springframework.util.ClassUtils.isPresent(
                "org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration",
                getClass().getClassLoader()))
                .as("spring-boot-starter-flyway doit etre declare, pas seulement flyway-core")
                .isTrue();
    }

    @Test
    @DisplayName("le service est lie au loopback")
    void theServiceIsBoundToLoopback() {
        // /admin/dlt/replay republie dans un topic METIER et n'est pas authentifie (P12-T8).
        // Spring Boot ecoute par defaut sur 0.0.0.0 : sans ce bind, n'importe quelle machine du
        // reseau local peut injecter dans la saga. CLAUDE.md §1.
        assertThat(serverAddress).isEqualTo("127.0.0.1");
    }

    @Test
    @DisplayName("hors profil lab, l'injection de panne est neutre")
    void outsideTheLabProfileFaultInjectionIsNeutral() {
        // Si ce test echoue, un header pose par un producteur peut faire echouer ce service.
        assertThat(faultInjection).isSameAs(FaultInjection.DISABLED);
    }

    @Test
    @DisplayName("les trois listeners sont declares : topic principal et deux etages de retry")
    void theThreeListenersAreDeclared() {
        assertThat(registry.getListenerContainers()).hasSize(3);
        assertThat(registry.getListenerContainers().stream()
                .flatMap(container -> container.getContainerProperties().getTopics() == null
                        ? java.util.stream.Stream.<String>empty()
                        : java.util.Arrays.stream(container.getContainerProperties().getTopics()))
                .toList())
                .containsExactlyInAnyOrder(
                        "shop.order.events.v1",
                        "shop.order.events.v1.retry-1",
                        "shop.order.events.v1.retry-2");
    }
}

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

    @Test
    @DisplayName("le contexte demarre avec la mecanique de fiabilite branchee")
    void contextLoadsWithReliabilityWiredIn() {
        assertThat(handler).isNotNull();
        assertThat(processedEventStore).isNotNull();
        assertThat(retryStageSupport).isNotNull();
        assertThat(deadLetterPublishingRecoverer).isNotNull();
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

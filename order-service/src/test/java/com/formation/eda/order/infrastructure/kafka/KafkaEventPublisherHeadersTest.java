package com.formation.eda.order.infrastructure.kafka;

import com.formation.eda.events.EventHeaders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * L'intercepteur d'idempotence de la phase 4 vit dans common-kafka et lit le header pose ici.
 * Deux constantes recopiees a la main divergeraient d'un caractere sans que rien n'echoue :
 * le header serait simplement introuvable, et la deduplication silencieusement inactive.
 */
class KafkaEventPublisherHeadersTest {

    @Test
    @DisplayName("le publisher utilise les constantes partagees de common-events")
    void publisherUsesSharedConstants() {
        assertThat(KafkaEventPublisher.HEADER_EVENT_ID).isEqualTo(EventHeaders.EVENT_ID);
        assertThat(KafkaEventPublisher.HEADER_EVENT_TYPE).isEqualTo(EventHeaders.EVENT_TYPE);
        assertThat(KafkaEventPublisher.HEADER_CORRELATION_ID).isEqualTo(EventHeaders.CORRELATION_ID);
    }
}

package com.formation.eda.events;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * L'enveloppe porte la tracabilite de toute la saga : si le chainage est faux, aucune trace
 * distribuee (phase 11) ne sera reconstituable. D'ou des tests des la phase 1.
 */
class EventEnvelopeTest {

    @Test
    @DisplayName("un evenement racine est sa propre correlation et n'a pas de cause")
    void rootEventStartsItsOwnCorrelation() {
        EventEnvelope root = EventEnvelope.root("OrderPlaced", "order-1", 1);

        assertThat(root.correlationId()).isEqualTo(root.eventId().toString());
        assertThat(root.causationId()).isNull();
        assertThat(root.isRoot()).isTrue();
    }

    @Test
    @DisplayName("un evenement consecutif garde la correlation et pointe sa cause")
    void causedEventKeepsCorrelationAndPointsToItsCause() {
        EventEnvelope root = EventEnvelope.root("OrderPlaced", "order-1", 1);

        EventEnvelope authorized = EventEnvelope.causedBy("PaymentAuthorized", "order-1", 1, root);

        assertThat(authorized.correlationId()).isEqualTo(root.correlationId());
        assertThat(authorized.causationId()).isEqualTo(root.eventId().toString());
        assertThat(authorized.eventId()).isNotEqualTo(root.eventId());
        assertThat(authorized.isRoot()).isFalse();
    }

    @Test
    @DisplayName("la correlation survit a toute la chaine, pas seulement au premier saut")
    void correlationSurvivesTheWholeChain() {
        EventEnvelope placed = EventEnvelope.root("OrderPlaced", "order-1", 1);
        EventEnvelope authorized = EventEnvelope.causedBy("PaymentAuthorized", "order-1", 1, placed);
        EventEnvelope reserved = EventEnvelope.causedBy("StockReserved", "SKU-1", 1, authorized);
        EventEnvelope shipped = EventEnvelope.causedBy("ShipmentCreated", "order-1", 1, reserved);

        assertThat(shipped.correlationId()).isEqualTo(placed.correlationId());
        // Le causationId, lui, change a chaque saut : c'est le chainon precedent.
        assertThat(shipped.causationId()).isEqualTo(reserved.eventId().toString());
    }

    @Test
    @DisplayName("une enveloppe sans agregat ou sans type est refusee a la construction")
    void invalidEnvelopesAreRejected() {
        assertThatThrownBy(() -> EventEnvelope.root("OrderPlaced", "  ", 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("aggregateId");

        assertThatThrownBy(() -> EventEnvelope.root("", "order-1", 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("eventType");

        assertThatThrownBy(() -> EventEnvelope.root("OrderPlaced", "order-1", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("version");
    }
}

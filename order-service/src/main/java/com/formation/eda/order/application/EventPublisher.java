package com.formation.eda.order.application;

import com.formation.eda.events.DomainEvent;

/**
 * Port de sortie : « publier un evenement », sans dire comment.
 *
 * <p>WHY: la couche application ne connait pas Kafka. Le cas d'usage « placer une commande »
 * se teste alors sans broker (cf. {@code PlaceOrderServiceTest}), et le passage a l'outbox en
 * phase 5 remplacera l'implementation sans toucher au cas d'usage.
 */
public interface EventPublisher {

    /** Ou l'evenement a atterri. Expose en phase 1 pour observer le partitionnement. */
    record Publication(String topic, int partition, long offset) {
    }

    Publication publish(String topic, DomainEvent event);
}

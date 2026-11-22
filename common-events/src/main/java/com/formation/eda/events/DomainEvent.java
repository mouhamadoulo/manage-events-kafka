package com.formation.eda.events;

/**
 * Contrat commun a tous les evenements metier.
 *
 * <p>Deliberement minimal : une enveloppe, et une cle de partition. Rien d'autre n'est
 * commun a un {@code OrderPlaced} et a un {@code StockReserved}.
 *
 * <p>Interface simple et non {@code sealed} : la liste des evenements grandit a chaque
 * phase, et un {@code permits} a maintenir dans common-events obligerait a modifier ce
 * fichier pour chaque nouvel evenement d'un autre module.
 */
public interface DomainEvent {

    EventEnvelope envelope();

    /**
     * Cle de partition Kafka de cet evenement.
     *
     * <p>WHY: la cle est declaree par l'evenement lui-meme, pas par le code qui publie.
     * Kafka ne garantit l'ordre qu'<b>a l'interieur d'une partition</b>, et la cle decide
     * de la partition. Mettre ce choix dans l'evenement evite qu'un appelant publie deux
     * evenements du meme agregat sous deux cles differentes : ils partiraient sur deux
     * partitions et pourraient etre consommes dans le desordre.
     *
     * <p>Toutes les cles ne sont pas l'aggregateId : les evenements de stock sont cles sur
     * le {@code sku}, car l'ordre qui compte la-bas est celui des mouvements d'un article
     * (cf. tableau des topics, CLAUDE.md §2).
     */
    String partitionKey();

    default String eventType() {
        return envelope().eventType();
    }
}

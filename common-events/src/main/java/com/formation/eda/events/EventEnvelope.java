package com.formation.eda.events;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Enveloppe technique portee par tout evenement du systeme (cf. CLAUDE.md §4).
 *
 * <p>Le payload repond a « qu'est-ce qui s'est passe ». L'enveloppe repond a
 * « quand, pour quel agregat, dans quel enchainement, et sous quelle forme ».
 * Les deux sont separes pour qu'un champ technique ajoute plus tard ne touche
 * pas la forme metier des evenements.
 *
 * @param eventId       identite unique de CETTE publication. Base de l'idempotence
 *                      consumer (table {@code processed_event}, phase 4) : deux copies
 *                      du meme evenement portent le meme eventId et ne doivent etre
 *                      traitees qu'une fois.
 * @param eventType     nom logique de l'evenement, pas le nom de classe Java. Le
 *                      consumer route sur cette chaine ; renommer la classe ne doit
 *                      pas casser les consumers.
 * @param occurredAt    instant du fait metier, decide par le producteur. Different de
 *                      l'horodatage Kafka du record, qui est l'instant de publication.
 *                      L'ecart entre les deux est la latence de la chaine.
 * @param correlationId identifiant de la transaction metier entiere. Constant sur toute
 *                      la saga : les ~12 evenements d'une commande partagent le meme.
 *                      Sert a reconstituer un flux dans les logs (phase 11).
 * @param causationId   eventId de l'evenement qui a cause celui-ci, {@code null} pour un
 *                      evenement racine. correlationId dit « quelle affaire », causationId
 *                      dit « qui a declenche cette etape precise » : l'un donne le groupe,
 *                      l'autre l'arbre.
 * @param aggregateId   identite de l'agregat concerne. Determine quasi toujours la cle de
 *                      partition, donc l'ordre (cf. {@link DomainEvent#partitionKey()}).
 * @param version       version du CONTRAT de l'evenement, a partir de 1. Ce n'est pas le
 *                      numero de sequence de l'agregat : en phase 8 l'event store ajoutera
 *                      un champ distinct pour cela. Ici, version sert a l'upcasting
 *                      (lire un v1 avec du code qui attend un v2).
 */
public record EventEnvelope(
        UUID eventId,
        String eventType,
        Instant occurredAt,
        String correlationId,
        String causationId,
        String aggregateId,
        int version) {

    public EventEnvelope {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(occurredAt, "occurredAt");
        requireText(eventType, "eventType");
        requireText(correlationId, "correlationId");
        requireText(aggregateId, "aggregateId");
        // causationId reste nullable : un evenement racine n'a pas de cause.
        if (version < 1) {
            throw new IllegalArgumentException("version doit etre >= 1, recu : " + version);
        }
    }

    /**
     * Enveloppe d'un evenement <b>racine</b> : rien dans le systeme ne l'a cause, il vient
     * d'une action externe (un POST /orders).
     *
     * <p>WHY: le correlationId reprend l'eventId. Une transaction metier doit avoir un
     * identifiant des son premier evenement, sinon les evenements suivants n'ont rien a
     * propager et la trace de la saga est perdue. Reutiliser l'eventId evite d'inventer un
     * second UUID qui designerait la meme chose.
     */
    public static EventEnvelope root(String eventType, String aggregateId, int version) {
        UUID eventId = UUID.randomUUID();
        return new EventEnvelope(
                eventId,
                eventType,
                Instant.now(),
                eventId.toString(),
                null,
                aggregateId,
                version);
    }

    /**
     * Enveloppe d'un evenement <b>consecutif</b> : produit en reaction a {@code cause}.
     *
     * <p>WHY: correlationId est recopie (meme affaire), causationId prend l'eventId de la
     * cause (nouveau chainon). C'est ce qui permet, a partir d'un seul evenement, de
     * remonter toute la chaine et de detecter une boucle.
     */
    public static EventEnvelope causedBy(String eventType, String aggregateId, int version, EventEnvelope cause) {
        Objects.requireNonNull(cause, "cause");
        return new EventEnvelope(
                UUID.randomUUID(),
                eventType,
                Instant.now(),
                cause.correlationId(),
                cause.eventId().toString(),
                aggregateId,
                version);
    }

    /**
     * Un evenement racine n'a pas de cause : utile pour verifier l'absence de boucle.
     *
     * <p>WHY {@code @JsonIgnore} : un accesseur prefixe {@code is}/{@code get} est vu par
     * Jackson comme une propriete, meme sur un record. Sans cette annotation, le JSON publie
     * contient un champ {@code "root": true} que personne n'a declare dans le contrat, et
     * qui partirait tel quel dans le Schema Registry en phase 2. Regle generale : sur un
     * record d'evenement, toute methode derivee doit etre soit ignoree, soit nommee sans
     * prefixe de getter.
     */
    @JsonIgnore
    public boolean isRoot() {
        return causationId == null;
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " ne peut pas etre vide");
        }
    }
}

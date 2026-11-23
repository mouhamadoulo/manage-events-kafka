package com.formation.eda.events;

/**
 * Noms des headers Kafka portes par les evenements du systeme.
 *
 * <p>WHY des headers alors que l'enveloppe est deja dans le payload : un header se lit
 * <b>sans deserialiser</b> le message. Un routeur, une DLQ, un filtre de Kafka UI ou
 * l'intercepteur d'idempotence de la phase 4 peuvent trier sans connaitre la classe Java du
 * payload — ni meme pouvoir le decoder, ce qui est precisement le cas d'un message dont la
 * deserialisation a echoue. Le payload reste la source de verite ; les headers sont une
 * commodite de transport.
 *
 * <p>WHY ici et non dans {@code common-kafka} : un nom de header est un element de
 * <b>contrat</b>, au meme titre qu'un nom de champ du schema JSON. Un consumer ecrit dans un
 * autre langage lit ces noms sans rien connaitre de notre configuration Spring. Ce module n'a
 * d'ailleurs aucune dependance {@code kafka-clients} : on trouve donc ici les noms seuls, et
 * la lecture d'un {@code org.apache.kafka.common.header.Headers} dans
 * {@code com.formation.eda.kafka.reliability.RecordHeaders}.
 *
 * <p>Deux familles, distinguees par le prefixe :
 * <ul>
 *   <li><b>sans prefixe</b> — headers de contrat, poses par le producteur a chaque
 *       publication, recopies tels quels par tout rejeu ;
 *   <li><b>{@code x-}</b> — headers de diagnostic et d'exploitation, poses par la mecanique de
 *       fiabilite (phase 4). Ils ne font pas partie du contrat metier et ne doivent pas etre
 *       republies sur un topic metier.
 * </ul>
 */
public final class EventHeaders {

    /** {@code eventId} de l'enveloppe. Base de l'idempotence consumer (phase 4). */
    public static final String EVENT_ID = "eventId";

    /** Nom logique de l'evenement. Permet de router sans deserialiser. */
    public static final String EVENT_TYPE = "eventType";

    /** Identifiant de la transaction metier entiere, constant sur toute la saga. */
    public static final String CORRELATION_ID = "correlationId";

    /**
     * Topic metier d'origine, pose <b>une seule fois</b> lors du premier passage en retry.
     *
     * <p>WHY et non les headers {@code kafka_dlt-original-*} du framework : ceux-la sont
     * reecrits a chaque etage de retry. Au bout de deux etages ils designent {@code .retry-1},
     * pas le topic metier. Le rejeu depuis la DLT a besoin du point de depart.
     */
    public static final String ORIGIN_TOPIC = "x-origin-topic";

    /**
     * Horodatage de publication d'origine, en millisecondes epoch, pose une seule fois.
     *
     * <p>WHY: l'echeance d'un etage de retry se compte depuis la publication initiale, pas
     * depuis l'arrivee dans l'etage. Un delai compte a l'arrivee repartirait de zero a chaque
     * redemarrage du consumer.
     */
    public static final String ORIGIN_TIMESTAMP = "x-origin-timestamp";

    /** Etage de retry d'ou vient le message : {@code 0} topic principal, puis {@code 1}, {@code 2}. */
    public static final String RETRY_STAGE = "x-retry-stage";

    /** Instant de la mise en dead letter topic. */
    public static final String FAILED_AT = "x-failed-at";

    /**
     * Injection de panne, profil {@code lab} uniquement : le handler echoue les N premieres
     * fois qu'il voit cet {@code eventId}, puis reussit.
     *
     * <p>WHY un header et non un champ du payload : le schema JSON est ferme
     * ({@code additionalProperties: false}, cf. P2-T5), un champ en plus serait rejete par le
     * serialiseur. Et hors profil {@code lab} ce header est ignore — une panne ne doit pas
     * pouvoir etre commandee depuis un message.
     */
    public static final String FAIL_TIMES = "x-fail-times";

    /** Injection de panne non-retryable, profil {@code lab} uniquement. */
    public static final String FAIL_FATAL = "x-fail-fatal";

    private EventHeaders() {
    }
}

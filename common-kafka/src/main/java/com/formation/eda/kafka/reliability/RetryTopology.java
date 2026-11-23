package com.formation.eda.kafka.reliability;

/**
 * Nommage des topics de retry et de dead letter, et chemin d'un message en echec.
 *
 * <p>Convention imposee par l'infra (CLAUDE.md §2, topics crees en P0-T6, sujets enregistres en
 * P2-T3) :
 * <pre>
 *   shop.order.events.v1            topic metier
 *   shop.order.events.v1.retry-1    premier etage
 *   shop.order.events.v1.retry-2    second etage
 *   shop.order.events.v1.dlt        dead letter topic
 * </pre>
 *
 * <p>WHY ces noms et pas ceux de {@code @RetryableTopic} : l'annotation de spring-kafka colle
 * le separateur {@code -} en dur ({@code DestinationTopicPropertiesFactory
 * .joinWithRetrySuffix}, verifie par {@code javap}) et indexe a partir de 0. Avec un suffixe
 * {@code .retry} elle produirait {@code .retry-0} et {@code .retry-1} : decales d'un cran par
 * rapport aux 12 topics deja crees et aux sujets deja enregistres.
 *
 * <p>Classe purement fonctionnelle : aucun etat, aucune dependance Spring. Le nombre d'etages
 * est <b>passe en parametre</b> et non devine, pour que la topologie et les delais configures
 * ({@code eda.reliability.stage-delays}) ne puissent pas se contredire — une topologie qui
 * produirait un {@code .retry-3} que personne ne consomme perdrait les messages en silence.
 */
public final class RetryTopology {

    static final String RETRY_INFIX = ".retry-";
    static final String DLT_SUFFIX = ".dlt";

    /**
     * Destination d'un message qui vient d'echouer sur {@code topic}.
     *
     * @param topic      topic d'ou vient le record
     * @param stageCount nombre d'etages de retry declares (cf.
     *                   {@link ConsumerReliabilityProperties#stageCount()})
     * @return l'etage suivant, ou le dead letter topic si le dernier etage est atteint
     * @throws IllegalArgumentException si {@code topic} est deja un dead letter topic — il n'y
     *         a rien apres, et republier une DLT sur elle-meme serait une boucle infinie
     */
    public static String next(String topic, int stageCount) {
        if (topic.endsWith(DLT_SUFFIX)) {
            throw new IllegalArgumentException(
                    "un dead letter topic n'a pas de destination suivante : " + topic);
        }
        int stage = stageOf(topic);
        if (stage >= stageCount) {
            return deadLetterOf(topic);
        }
        return baseTopicOf(topic) + RETRY_INFIX + (stage + 1);
    }

    /**
     * @return {@code 0} pour un topic metier, {@code n} pour un {@code .retry-n}, {@code -1}
     *         pour un dead letter topic. Un suffixe {@code .retry-} sans numero lisible rend
     *         {@code 0} : mieux vaut traiter le topic comme un topic metier que calculer une
     *         echeance sur un numero invente.
     */
    public static int stageOf(String topic) {
        if (topic.endsWith(DLT_SUFFIX)) {
            return -1;
        }
        int at = topic.lastIndexOf(RETRY_INFIX);
        if (at < 0) {
            return 0;
        }
        try {
            return Integer.parseInt(topic.substring(at + RETRY_INFIX.length()));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /** @return le topic metier, sans suffixe d'etage ni de dead letter. */
    public static String baseTopicOf(String topic) {
        if (topic.endsWith(DLT_SUFFIX)) {
            return topic.substring(0, topic.length() - DLT_SUFFIX.length());
        }
        int at = topic.lastIndexOf(RETRY_INFIX);
        if (at < 0 || stageOf(topic) == 0) {
            return topic;
        }
        return topic.substring(0, at);
    }

    /**
     * @return le dead letter topic du topic metier dont {@code topic} fait partie.
     *
     * <p>Depuis {@code ....retry-2}, c'est {@code ....dlt} et non {@code ....retry-2.dlt} : il
     * y a une seule DLT par topic metier. Un sujet de Schema Registry par etage existe deja
     * (P2-T3), mais une DLT par etage multiplierait les endroits a surveiller.
     */
    public static String deadLetterOf(String topic) {
        return baseTopicOf(topic) + DLT_SUFFIX;
    }

    private RetryTopology() {
    }
}

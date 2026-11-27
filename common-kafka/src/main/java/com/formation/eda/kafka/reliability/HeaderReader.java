package com.formation.eda.kafka.reliability;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;

/**
 * Lecture typee d'un header Kafka, sans jamais lever.
 *
 * <p>WHY cette regle du « jamais lever » : toute la mecanique de fiabilite de la phase 4 route
 * sur des headers, et elle tourne dans l'intercepteur, donc <b>avant</b> le classement
 * d'erreur du conteneur. Une exception ici ne serait ni reessayee, ni envoyee en DLT : elle
 * remonterait au conteneur et bloquerait la partition — exactement ce que cette phase supprime.
 * Un header absent ou illisible est une anomalie a journaliser, pas un echec de traitement.
 *
 * <p>Un header Kafka est un {@code byte[]} sans type. Nous ecrivons tout en texte UTF-8
 * plutot qu'en binaire : un operateur doit pouvoir lire un header dans Kafka UI ou dans
 * {@code kafka-console-consumer --property print.headers=true} sans decodeur.
 *
 * <p>La classe s'appelle {@code HeaderReader} et non {@code RecordHeaders} parce que
 * {@code org.apache.kafka.common.header.internals.RecordHeaders} existe deja : deux classes
 * de meme nom dans un fichier qui a besoin des deux obligent a qualifier l'une des deux.
 */
public final class HeaderReader {

    /**
     * @return la valeur du <b>dernier</b> header de ce nom, ou {@code null} s'il est absent.
     *
     * <p>WHY la derniere et non la premiere : Kafka autorise plusieurs valeurs sous un meme
     * nom, et {@link org.springframework.kafka.listener.DeadLetterPublishingRecoverer} ajoute
     * ses headers d'origine a chaque etage de retry au lieu de les remplacer. Pour un header
     * que nous posons nous-memes une seule fois, la derniere valeur est la plus recente.
     */
    public static String string(Headers headers, String name) {
        Header header = headers.lastHeader(name);
        return header == null || header.value() == null
                ? null
                : new String(header.value(), StandardCharsets.UTF_8);
    }

    /** @return la valeur en {@code long}, vide si le header est absent ou illisible. */
    public static OptionalLong longValue(Headers headers, String name) {
        String raw = string(headers, name);
        if (raw == null) {
            return OptionalLong.empty();
        }
        try {
            return OptionalLong.of(Long.parseLong(raw.trim()));
        } catch (NumberFormatException e) {
            return OptionalLong.empty();
        }
    }

    /** @return la valeur en {@code int}, ou {@code defaultValue} si absente ou illisible. */
    public static int intValue(Headers headers, String name, int defaultValue) {
        String raw = string(headers, name);
        if (raw == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private HeaderReader() {
    }
}

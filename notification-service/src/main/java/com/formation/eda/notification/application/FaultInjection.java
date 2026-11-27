package com.formation.eda.notification.application;

import org.apache.kafka.common.header.Headers;

import java.util.UUID;

/**
 * Injection de panne a la demande, pour rendre la phase 4 verifiable.
 *
 * <p>WHY ce mecanisme existe : l'effet metier de ce service est un compteur en memoire. Rien
 * ne peut echouer. Sans declencheur d'echec, ni les etages de retry, ni la branche « erreur
 * metier » de la DLT, ni le rejeu selectif n'auraient de quoi etre mesures — on les aurait
 * ecrits sans jamais les voir marcher.
 *
 * <p>WHY un header et non un champ du payload : le schema JSON est ferme
 * ({@code additionalProperties: false}, cf. P2-T5), un champ en plus serait refuse par le
 * serialiseur. Et un header se lit sans deserialiser, ce qui est deja le parti pris du projet.
 *
 * <p>WHY une interface avec une implementation neutre : le header vient du producteur, donc de
 * l'exterieur. Hors profil {@code lab}, c'est {@link #DISABLED} qui est injecte et les headers
 * sont ignores — <b>une panne ne doit pas pouvoir etre commandee depuis un message</b>.
 */
public interface FaultInjection {

    /** N'injecte jamais rien. C'est l'implementation active hors profil {@code lab}. */
    FaultInjection DISABLED = (headers, eventId) -> {
    };

    /**
     * @throws com.formation.eda.kafka.reliability.TransientProcessingException si
     *         {@code x-fail-times} n'est pas epuise pour cet {@code eventId}
     * @throws com.formation.eda.kafka.reliability.NonRetryableEventException si
     *         {@code x-fail-fatal} est pose
     */
    void check(Headers headers, UUID eventId);
}

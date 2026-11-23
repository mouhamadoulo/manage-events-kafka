package com.formation.eda.notification.application;

import com.formation.eda.events.EventHeaders;
import com.formation.eda.kafka.reliability.HeaderReader;
import com.formation.eda.kafka.reliability.NonRetryableEventException;
import com.formation.eda.kafka.reliability.TransientProcessingException;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Injection de panne pilotee par headers. <b>Profil {@code lab} uniquement</b> (cf.
 * {@code FaultInjectionConfiguration}).
 *
 * <p>{@code x-fail-times: N} fait echouer les N premiers passages de cet {@code eventId}, puis
 * laisse passer. C'est ce qui permet de mesurer un trajet complet : echec sur le topic
 * principal, attente de 5 s dans {@code .retry-1}, echec encore, attente de 30 s dans
 * {@code .retry-2}, puis succes — ou DLT si N est assez grand.
 *
 * <p>WHY un compte par {@code eventId} et non un compteur global : avec un compteur global, le
 * second message d'une rafale reussirait du premier coup et l'experience ne mesurerait plus
 * qu'un seul trajet.
 *
 * <p>Le compte vit en memoire, donc un redemarrage le remet a zero. Volontaire : une
 * experience se relance, elle ne se reprend pas.
 */
public class LabFaultInjection implements FaultInjection {

    private static final Logger log = LoggerFactory.getLogger(LabFaultInjection.class);

    private final Map<UUID, Integer> failuresLeft = new ConcurrentHashMap<>();

    @Override
    public void check(Headers headers, UUID eventId) {
        if (Boolean.parseBoolean(HeaderReader.string(headers, EventHeaders.FAIL_FATAL))) {
            throw new NonRetryableEventException(
                    "panne definitive injectee par " + EventHeaders.FAIL_FATAL);
        }

        int requested = HeaderReader.intValue(headers, EventHeaders.FAIL_TIMES, 0);
        if (requested <= 0 || eventId == null) {
            return;
        }

        // compute pour que lecture et decrement soient atomiques : deux partitions du meme
        // etage peuvent etre traitees en parallele.
        int left = failuresLeft.compute(eventId,
                (id, remaining) -> (remaining == null ? requested : remaining) - 1);
        if (left >= 0) {
            log.info("panne transitoire injectee eventId={} restant={}", eventId, left);
            throw new TransientProcessingException(
                    "panne transitoire injectee, reste " + left + " echec(s)", null);
        }
    }
}

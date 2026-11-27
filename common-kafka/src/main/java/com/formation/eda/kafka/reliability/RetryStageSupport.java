package com.formation.eda.kafka.reliability;

import com.formation.eda.events.EventHeaders;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.OptionalLong;

/**
 * Echeance d'un record dans un etage de retry.
 *
 * <p>Un listener d'etage appelle {@link #requireDue} en premiere ligne. S'il ne leve pas, le
 * record est du et le traitement continue.
 */
public class RetryStageSupport {

    private static final Logger log = LoggerFactory.getLogger(RetryStageSupport.class);

    private final ConsumerReliabilityProperties props;
    private final Clock clock;

    public RetryStageSupport(ConsumerReliabilityProperties props, Clock clock) {
        this.props = props;
        this.clock = clock;
    }

    /**
     * @throws NotYetDueException si l'echeance de l'etage n'est pas atteinte. La duree restante
     *         portee par l'exception devient l'intervalle de pause de la partition.
     * @throws IllegalArgumentException si le record vient d'un etage pour lequel aucun delai
     *         n'est declare
     */
    public void requireDue(ConsumerRecord<?, ?> record) {
        int stage = RetryTopology.stageOf(record.topic());
        if (stage <= 0) {
            // Topic metier, ou dead letter topic : aucune echeance a respecter.
            return;
        }

        // Leve si l'etage n'a pas de delai configure. WHY echouer plutot que prendre le
        // dernier delai connu : un .retry-3 face a deux delais declares signale une infra et
        // une configuration qui ont divergé. Inventer une echeance masquerait l'ecart.
        Duration delay = props.delayOfStage(stage);

        OptionalLong origin = HeaderReader.longValue(record.headers(), EventHeaders.ORIGIN_TIMESTAMP);
        if (origin.isEmpty()) {
            // Message publie a la main, ou producteur qui ne pose pas le header. Une echeance
            // incalculable ne doit pas mettre le record en attente indefinie.
            log.warn("pas de header {} sur {}-{} offset={} : traitement immediat, sans attendre les {} de l'etage {}",
                    EventHeaders.ORIGIN_TIMESTAMP, record.topic(), record.partition(),
                    record.offset(), delay, stage);
            return;
        }

        // WHY depuis le header et non depuis record.timestamp() : le timestamp du record est
        // l'instant de republication DANS l'etage. Compter depuis lui ferait repartir le delai
        // de zero a chaque republication, et un message pourrait attendre indefiniment.
        long delayMillis = delay.toMillis();
        long now = clock.millis();
        long originMillis = origin.getAsLong();

        // Ce header vient du PRODUCTEUR : c'est une valeur non fiable, et elle decide combien
        // de temps une partition reste suspendue.
        //   - une valeur negative ou absurde fait deborder originMillis + delayMillis, et le
        //     SIGNE du resultat deciderait du comportement par accident ;
        //   - une valeur tres future (annee 2100) suspendrait la partition jusque-la, donc
        //     arreterait la consommation de l'etage avec un seul message.
        // Hors plage, on traite immediatement : un message traite trop tot est un moindre mal
        // qu'un etage de retry mort.
        if (originMillis < 0 || originMillis > now + delayMillis) {
            log.warn("header {} hors plage ({}) sur {}-{} offset={} : traitement immediat",
                    EventHeaders.ORIGIN_TIMESTAMP, originMillis, record.topic(),
                    record.partition(), record.offset());
            return;
        }

        // Borne dure : l'attente ne depasse jamais le delai de l'etage, meme si les horloges du
        // producteur et du consumer ont derive.
        long remaining = Math.min(originMillis + delayMillis - now, delayMillis);
        if (remaining > 0) {
            // La partition voyage dans l'exception : le BackOffHandler qui appliquera la pause
            // ne recoit pas le record, et n'a aucun autre moyen de savoir quoi suspendre.
            throw new NotYetDueException(remaining,
                    new TopicPartition(record.topic(), record.partition()));
        }
    }
}

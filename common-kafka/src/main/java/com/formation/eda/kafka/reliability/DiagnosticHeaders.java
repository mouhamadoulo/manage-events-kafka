package com.formation.eda.kafka.reliability;

import com.formation.eda.events.EventHeaders;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.function.BiFunction;

/**
 * Headers ajoutes a un message republie vers un etage de retry ou vers la DLT.
 *
 * <p>C'est la {@code setHeadersFunction} du {@code DeadLetterPublishingRecoverer}. Elle ne
 * remplace pas les headers du framework ({@code kafka_dlt-exception-fqcn},
 * {@code kafka_dlt-original-offset}, la trace...) : elle les complete avec ce dont le rejeu et
 * le calcul d'echeance ont besoin, et que le framework ne fournit pas de facon utilisable.
 *
 * <p>WHY nos propres headers alors que le framework en pose deja : les
 * {@code kafka_dlt-original-*} sont <b>reecrits a chaque etage</b>. Apres deux etages, ils
 * designent {@code .retry-1} et non le topic metier. Un rejeu qui s'y fierait republierait sur
 * un topic de retry. Les deux headers qui doivent survivre au trajet entier — le topic
 * d'origine et l'instant de publication d'origine — sont donc poses <b>une seule fois</b>, au
 * premier passage, et jamais ecrases ensuite.
 */
public class DiagnosticHeaders implements BiFunction<ConsumerRecord<?, ?>, Exception, Headers> {

    private final Clock clock;

    public DiagnosticHeaders(Clock clock) {
        this.clock = clock;
    }

    @Override
    public Headers apply(ConsumerRecord<?, ?> record, Exception exception) {
        Headers added = new RecordHeaders();

        // Poses une seule fois. Le recoverer recopie les headers du record d'origine sur le
        // message republie : s'ils sont deja la, les reposer creerait une seconde valeur sous
        // le meme nom, et le lecteur devrait deviner laquelle est la bonne.
        if (record.headers().lastHeader(EventHeaders.ORIGIN_TOPIC) == null) {
            put(added, EventHeaders.ORIGIN_TOPIC, record.topic());
        }
        if (record.headers().lastHeader(EventHeaders.ORIGIN_TIMESTAMP) == null) {
            // Le timestamp du record au PREMIER echec, donc l'instant de publication sur le
            // topic metier. C'est de la que se comptent les echeances des etages.
            put(added, EventHeaders.ORIGIN_TIMESTAMP, String.valueOf(record.timestamp()));
        }

        // Reposes a chaque etage, eux : ils decrivent ce passage-ci.
        put(added, EventHeaders.RETRY_STAGE, String.valueOf(RetryTopology.stageOf(record.topic())));
        put(added, EventHeaders.FAILED_AT, Instant.now(clock).toString());

        return added;
    }

    private static void put(Headers headers, String name, String value) {
        headers.add(name, value.getBytes(StandardCharsets.UTF_8));
    }
}

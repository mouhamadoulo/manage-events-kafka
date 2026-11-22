package com.formation.eda.order.infrastructure.kafka;

import com.formation.eda.events.DomainEvent;
import com.formation.eda.events.EventEnvelope;
import com.formation.eda.order.application.EventPublicationFailedException;
import com.formation.eda.order.application.EventPublisher;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Publication d'un evenement sur Kafka.
 *
 * <p>Phase 3 : le producteur est configure pour ne pas perdre et ne pas dupliquer
 * (cf. {@code application.yml}, section {@code producer}). Ce qui reste a la charge du code,
 * ici, c'est de <b>regarder le resultat de l'envoi</b>.
 *
 * <p>Phase 5 : toute publication issue d'un changement d'etat en base passera par l'outbox
 * transactionnel. Ici, si l'envoi echoue apres que la commande a ete acceptee, il n'y a rien
 * pour rattraper — c'est le dual-write problem, et aucune configuration de producteur ne le
 * resout.
 */
@Component
public class KafkaEventPublisher implements EventPublisher {

    // L'enveloppe est dans le payload, mais ces trois champs sont AUSSI en headers.
    // WHY: un header se lit sans deserialiser le message. Un routeur, une DLQ ou un filtre de
    // l'UI peuvent trier sur eventType sans connaitre la classe Java du payload. Le payload
    // reste la source de verite ; les headers sont une commodite de transport.
    public static final String HEADER_EVENT_ID = "eventId";
    public static final String HEADER_EVENT_TYPE = "eventType";
    public static final String HEADER_CORRELATION_ID = "correlationId";

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Envoi synchrone : on attend l'accuse du broker avant de repondre au client HTTP.
     *
     * <p>WHY attendre : {@code send()} rend la main des que le record est dans le buffer du
     * producteur, pas quand il est ecrit. Repondre 202 sans attendre voudrait dire « acceptee »
     * alors que l'evenement peut encore echouer — et il n'y a ici aucune outbox pour le
     * rattraper. Le cout est assume : le thread HTTP attend le reseau, et le debit d'un envoi
     * unitaire est mediocre (cf. P3-T5, qui mesure l'ecart avec un envoi en rafale).
     */
    @Override
    public Publication publish(String topic, DomainEvent event) {
        EventEnvelope envelope = event.envelope();
        try {
            SendResult<String, Object> result = send(topic, event).join();
            RecordMetadata metadata = result.getRecordMetadata();

            log.info("publie {} eventId={} cle={} -> {}-{} offset={}",
                    envelope.eventType(),
                    envelope.eventId(),
                    event.partitionKey(),
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset());

            return new Publication(metadata.topic(), metadata.partition(), metadata.offset());
        } catch (CompletionException e) {
            // join() emballe la cause dans une CompletionException. La deballer evite une
            // trace a deux etages et laisse voir l'exception Kafka reelle
            // (TimeoutException, RecordTooLargeException, SerializationException...).
            Throwable cause = e.getCause() == null ? e : e.getCause();
            log.error("echec de publication {} eventId={} cle={} : {}",
                    envelope.eventType(), envelope.eventId(), event.partitionKey(),
                    cause.toString());
            throw new EventPublicationFailedException(
                    "publication de " + envelope.eventType() + " refusee : " + cause.getMessage(),
                    cause);
        }
    }

    /**
     * Envoi asynchrone, avec un <b>callback d'echec obligatoire</b>.
     *
     * <p>WHY ce garde-fou : {@code kafkaTemplate.send(record)} sans rien faire du
     * {@code CompletableFuture} compile, passe les tests et perd les messages en silence. Un
     * echec de livraison n'apparait alors ni dans les logs, ni dans une metrique, ni pour le
     * client. C'est le piege le plus courant du producteur Kafka, et il ne se voit pas a la
     * relecture : il n'y a <b>rien</b> a lire.
     *
     * <p>Utilise par l'outil de rafale des experiences (P3-T3 a P3-T5), ou attendre chaque
     * accuse un par un rendrait toute mesure de debit absurde.
     */
    public CompletableFuture<SendResult<String, Object>> publishAsync(String topic, DomainEvent event) {
        EventEnvelope envelope = event.envelope();
        return send(topic, event).whenComplete((result, failure) -> {
            if (failure != null) {
                log.error("echec de publication asynchrone {} eventId={} : {}",
                        envelope.eventType(), envelope.eventId(), failure.toString());
            }
        });
    }

    private CompletableFuture<SendResult<String, Object>> send(String topic, DomainEvent event) {
        EventEnvelope envelope = event.envelope();

        // Partition laissee a null : c'est le partitioner qui choisit, a partir du hash de la
        // cle. Imposer un numero de partition ici reviendrait a recoder le partitionnement a
        // la main et a casser la repartition des qu'on ajoute des partitions.
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(topic, null, event.partitionKey(), event);
        header(record, HEADER_EVENT_ID, envelope.eventId().toString());
        header(record, HEADER_EVENT_TYPE, envelope.eventType());
        header(record, HEADER_CORRELATION_ID, envelope.correlationId());

        return kafkaTemplate.send(record);
    }

    private static void header(ProducerRecord<String, ?> record, String name, String value) {
        if (value != null) {
            record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
        }
    }
}

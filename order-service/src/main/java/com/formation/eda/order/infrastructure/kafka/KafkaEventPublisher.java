package com.formation.eda.order.infrastructure.kafka;

import com.formation.eda.events.DomainEvent;
import com.formation.eda.events.EventEnvelope;
import com.formation.eda.order.application.EventPublisher;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Publication d'un evenement sur Kafka.
 *
 * <p>Phase 1 : envoi direct, synchrone. A partir de la phase 5, toute publication issue d'un
 * changement d'etat en base passera par l'<b>outbox transactionnel</b> : ici, si le send
 * echoue apres que la commande a ete acceptee, l'evenement est perdu sans que personne ne le
 * sache. C'est exactement le dual-write problem que la phase 5 corrige.
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

    @Override
    public Publication publish(String topic, DomainEvent event) {
        EventEnvelope envelope = event.envelope();

        // Partition laissee a null : c'est le partitioner qui choisit, a partir du hash de la
        // cle. Imposer un numero de partition ici reviendrait a recoder le partitionnement a
        // la main et a casser la repartition des qu'on ajoute des partitions.
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(topic, null, event.partitionKey(), event);
        header(record, HEADER_EVENT_ID, envelope.eventId().toString());
        header(record, HEADER_EVENT_TYPE, envelope.eventType());
        header(record, HEADER_CORRELATION_ID, envelope.correlationId());

        // join() : on attend l'accuse du broker. WHY en phase 1 : on veut la partition et
        // l'offset reellement attribues pour les observer (experience P1-T4). Le cout est
        // reel — le thread HTTP attend le reseau — et la phase 3 reviendra sur acks, batching
        // et envoi asynchrone.
        SendResult<String, Object> result = kafkaTemplate.send(record).join();
        RecordMetadata metadata = result.getRecordMetadata();

        log.info("publie {} eventId={} cle={} -> {}-{} offset={}",
                envelope.eventType(),
                envelope.eventId(),
                event.partitionKey(),
                metadata.topic(),
                metadata.partition(),
                metadata.offset());

        return new Publication(metadata.topic(), metadata.partition(), metadata.offset());
    }

    private static void header(ProducerRecord<String, ?> record, String name, String value) {
        if (value != null) {
            record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
        }
    }
}

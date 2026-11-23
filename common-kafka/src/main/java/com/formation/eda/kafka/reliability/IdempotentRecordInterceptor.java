package com.formation.eda.kafka.reliability;

import com.formation.eda.events.EventHeaders;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.RecordInterceptor;

import java.util.UUID;

/**
 * Deduplication des evenements, posee devant le listener.
 *
 * <p>WHY un {@link RecordInterceptor} et non un garde-fou dans chaque listener : le point
 * d'extension existe deja, il s'applique a tous les listeners d'une fabrique de conteneurs, et
 * rendre {@code null} depuis {@code intercept()} fait que spring-kafka <b>n'appelle pas</b> le
 * listener et commite quand meme l'offset. C'est exactement le comportement voulu pour un
 * doublon : ne rien faire, et avancer.
 *
 * <p>WHY lire l'{@code eventId} dans un header et non dans le payload : le header se lit sans
 * deserialiser. Un record dont la deserialisation a echoue n'a pas de payload du tout.
 *
 * <h2>Le piege de cette classe</h2>
 *
 * <p>La marque est posee <b>avant</b> le traitement, et {@link #failure} la retire. Cet
 * equilibre tient a une condition : <b>tout chemin de sortie du listener doit lever</b>.
 * {@code failure()} n'est appele que sur exception. Un listener qui sort normalement sans avoir
 * travaille — par exemple en mettant son record en attente — laisse la marque en place, et au
 * passage suivant l'evenement est classe doublon : le travail est perdu <b>en silence</b>,
 * sans trace a lire.
 *
 * <p>C'est arrive dans ce projet meme : la premiere version du differe de retry faisait
 * {@code pause} + {@code seek} + retour normal, et perdait tous les messages en attente. D'ou
 * {@link NotYetDueException} — voir {@code docs/patterns/04-retry-dlq-idempotence.md}.
 *
 * <p>Limite assumee : un crash brutal entre le marquage et l'echec laisse une marque
 * orpheline. La reponse correcte est d'ecrire la marque dans la meme transaction que l'effet
 * metier, ce que la phase 5 (outbox) rend possible.
 */
public class IdempotentRecordInterceptor implements RecordInterceptor<String, Object> {

    private static final Logger log = LoggerFactory.getLogger(IdempotentRecordInterceptor.class);

    private final ProcessedEventStore store;

    /**
     * Marque posee par CE thread pour le record en cours, ou {@code null} s'il n'en a pose
     * aucune.
     *
     * <p>WHY ce suivi plutot que relire l'{@code eventId} du header dans {@link #failure} :
     * {@code failure()} ne doit defaire que ce que {@code intercept()} a fait. Sans cela, un
     * record passe <b>sans</b> deduplication — payload nul, header illisible — mais portant un
     * {@code eventId} deja traite effacerait la marque d'un travail deja fait. Une copie
     * illisible d'un evenement suffirait alors a le faire rejouer : exactement le doublon que
     * cette classe empeche, declenchable par un producteur bogue qui republie un payload casse
     * sous un eventId deja utilise.
     *
     * <p>Un {@link ThreadLocal} suffit : spring-kafka traite un record a la fois par thread de
     * consumer, et {@code intercept}, {@code failure} et {@code afterRecord} tournent sur ce
     * meme thread.
     */
    private final ThreadLocal<UUID> markedByThisThread = new ThreadLocal<>();

    public IdempotentRecordInterceptor(ProcessedEventStore store) {
        this.store = store;
    }

    @Override
    public ConsumerRecord<String, Object> intercept(ConsumerRecord<String, Object> record,
                                                    Consumer<String, Object> consumer) {
        markedByThisThread.remove();

        // Valeur nulle : soit un tombstone (legitime sur un topic compacte), soit un echec de
        // deserialisation passe par ErrorHandlingDeserializer. Dans les deux cas l'eventId du
        // header n'est pas une identite fiable : on laisse le listener et le classement
        // d'erreur decider, sans rien marquer.
        if (record.value() == null) {
            return record;
        }

        UUID eventId = eventIdOf(record);
        if (eventId == null) {
            return record;
        }

        String consumerGroup = consumer.groupMetadata().groupId();
        if (store.markProcessed(eventId, consumerGroup, record.topic())) {
            markedByThisThread.set(eventId);
            return record;
        }

        log.info("doublon saute eventId={} groupe={} topic={} partition={} offset={}",
                eventId, consumerGroup, record.topic(), record.partition(), record.offset());
        return null;
    }

    @Override
    public void failure(ConsumerRecord<String, Object> record, Exception exception,
                        Consumer<String, Object> consumer) {
        UUID marked = markedByThisThread.get();
        if (marked == null) {
            // Ce record n'a pas ete deduplique par nous : rien a defaire. Relire l'eventId du
            // header ici effacerait la marque d'un AUTRE traitement portant le meme eventId.
            return;
        }
        store.forget(marked, consumer.groupMetadata().groupId());
    }

    @Override
    public void afterRecord(ConsumerRecord<String, Object> record, Consumer<String, Object> consumer) {
        // Appele apres succes comme apres echec : c'est ici que le suivi par thread se nettoie,
        // sinon un record ulterieur traite sur le meme thread verrait la marque du precedent.
        markedByThisThread.remove();
    }

    /** @return l'eventId du header, ou {@code null} s'il est absent ou illisible. */
    private UUID eventIdOf(ConsumerRecord<String, ?> record) {
        String raw = HeaderReader.string(record.headers(), EventHeaders.EVENT_ID);
        if (raw == null) {
            // Choix assume : l'absence du header est une faute du PRODUCTEUR, pas un defaut du
            // message. Elle doit etre visible, pas fatale — refuser le message ferait echouer
            // un consumer a cause d'un bug situe ailleurs.
            log.warn("pas de header {} : traitement SANS deduplication. topic={} partition={} offset={}",
                    EventHeaders.EVENT_ID, record.topic(), record.partition(), record.offset());
            return null;
        }
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            // Lever ici remonterait au conteneur AVANT le classement d'erreur : ni retry, ni
            // DLT, partition bloquee. Exactement ce que la phase 4 supprime.
            log.warn("header {} illisible ({}) : traitement SANS deduplication. topic={} offset={}",
                    EventHeaders.EVENT_ID, raw, record.topic(), record.offset());
            return null;
        }
    }
}

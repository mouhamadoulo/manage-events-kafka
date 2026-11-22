package com.formation.eda.notification.infrastructure.kafka;

import com.formation.eda.events.Topics;
import com.formation.eda.events.order.OrderPlaced;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.TreeMap;

/**
 * Consumer terminal : notification-service lit les evenements de commande et n'en produit
 * aucun. Rien dans le systeme ne depend de lui — l'arreter ne casse pas la saga, les
 * messages s'accumulent et il rattrape a son redemarrage. C'est la demonstration la plus
 * simple du decouplage.
 *
 * <p>Phase 1 : log seulement, et surtout <b>aucune idempotence</b>. Kafka livre au moins une
 * fois : apres un rebalancing ou un redemarrage, les memes evenements peuvent repasser.
 * L'experience P1-T6 (reset d'offsets) le montre volontairement, et la phase 4 y remedie avec
 * la table {@code processed_event}.
 */
@Component
public class OrderEventsListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventsListener.class);

    private final AtomicLong received = new AtomicLong();

    // Compteur par partition : c'est la matiere des experiences P1-T4 (repartition des cles)
    // et P1-T5 (quelles partitions cette instance s'est vue assigner).
    private final Map<Integer, LongAdder> countsByPartition = new ConcurrentHashMap<>();

    /**
     * {@code groupId} vient de la configuration, pas d'une constante.
     *
     * <p>WHY: le groupe est ce qui determine le partage du travail. Deux instances avec le
     * meme groupe se partagent les partitions ; avec deux groupes differents, chacune recoit
     * <b>tout</b>. Pouvoir le changer par variable d'environnement permet de faire les deux
     * experiences (P1-T5) sans recompiler.
     */
    @KafkaListener(topics = Topics.ORDER_EVENTS, groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderEvent(ConsumerRecord<String, OrderPlaced> record) {
        OrderPlaced event = record.value();
        countsByPartition.computeIfAbsent(record.partition(), p -> new LongAdder()).increment();

        // Ecart entre l'instant du fait metier et l'instant de reception : la latence reelle
        // de la chaine, bout en bout. Le timestamp du record, lui, est l'instant de
        // publication cote broker.
        Duration lag = Duration.between(event.envelope().occurredAt(), Instant.now());

        log.info("recu #{} {} cle={} partition={} offset={} latence={} ms client={} total={} {}",
                received.incrementAndGet(),
                event.envelope().eventType(),
                record.key(),
                record.partition(),
                record.offset(),
                lag.toMillis(),
                event.customerId(),
                event.totalAmount(),
                event.currency());
    }

    /** Nombre d'evenements vus par CETTE instance depuis son demarrage (experience P1-T5). */
    public long receivedCount() {
        return received.get();
    }

    /** Repartition par partition, telle que vue par CETTE instance. */
    public SortedMap<Integer, Long> countsByPartition() {
        SortedMap<Integer, Long> snapshot = new TreeMap<>();
        countsByPartition.forEach((partition, counter) -> snapshot.put(partition, counter.sum()));
        return snapshot;
    }

    public void resetCounters() {
        received.set(0);
        countsByPartition.clear();
    }
}

package com.formation.eda.notification.application;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Compteurs d'observation des experiences de la phase 1 : combien d'evenements CETTE instance
 * a traites, et sur quelles partitions.
 *
 * <p>Separe du consumer pour une raison concrete : les deux modeles compares en P1-T7
 * ({@code @KafkaListener} et le binding fonctionnel Spring Cloud Stream) alimentent les memes
 * compteurs, et un seul des deux est actif a la fois selon le profil. L'endpoint
 * {@code /stats} ne doit pas savoir lequel tourne.
 *
 * <p>Ces compteurs sont remplaces en phase 11 par des metriques Micrometer, qui donnent la
 * meme information en standard et exportable.
 */
@Component
public class EventCounters {

    private final AtomicLong received = new AtomicLong();
    private final Map<Integer, LongAdder> byPartition = new ConcurrentHashMap<>();

    /** @return le rang de l'evenement dans cette instance, pratique pour les logs. */
    public long record(int partition) {
        byPartition.computeIfAbsent(partition, p -> new LongAdder()).increment();
        return received.incrementAndGet();
    }

    public long receivedCount() {
        return received.get();
    }

    public SortedMap<Integer, Long> countsByPartition() {
        SortedMap<Integer, Long> snapshot = new TreeMap<>();
        byPartition.forEach((partition, counter) -> snapshot.put(partition, counter.sum()));
        return snapshot;
    }

    public void reset() {
        received.set(0);
        byPartition.clear();
    }
}

package com.formation.eda.notification.api;

import com.formation.eda.notification.infrastructure.kafka.OrderEventsListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.SortedMap;

/**
 * Endpoint d'observation, pour les experiences de la phase 1.
 *
 * <p>Il n'a aucune valeur metier : il sert a lire, instance par instance, quelles partitions
 * cette instance a reellement traitees. Avec quatre instances lancees (P1-T5), comparer les
 * quatre reponses rend le rebalancing visible sans lire les logs.
 */
@RestController
public class StatsController {

    private final OrderEventsListener listener;
    private final String groupId;
    private final int port;

    public StatsController(
            OrderEventsListener listener,
            @Value("${spring.kafka.consumer.group-id}") String groupId,
            @Value("${server.port}") int port) {
        this.listener = listener;
        this.groupId = groupId;
        this.port = port;
    }

    public record Stats(String groupId, int port, long received, SortedMap<Integer, Long> byPartition) {
    }

    @GetMapping("/stats")
    public Stats stats() {
        return new Stats(groupId, port, listener.receivedCount(), listener.countsByPartition());
    }

    @GetMapping("/stats/reset")
    public Map<String, String> reset() {
        listener.resetCounters();
        return Map.of("status", "compteurs remis a zero");
    }
}

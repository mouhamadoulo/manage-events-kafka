package com.formation.eda.notification.api;

import com.formation.eda.notification.application.EventCounters;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
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

    private final EventCounters counters;
    private final String groupId;
    private final int port;
    private final String model;

    public StatsController(
            EventCounters counters,
            Environment environment,
            @Value("${spring.kafka.consumer.group-id}") String groupId,
            @Value("${server.port}") int port) {
        this.counters = counters;
        this.groupId = groupId;
        this.port = port;
        // Quel modele de consommation tourne reellement : les deux lisent le meme topic mais
        // dans deux groupes distincts, et /stats ne doit pas laisser croire le contraire.
        this.model = environment.matchesProfiles("cloud-stream") ? "spring-cloud-stream" : "spring-kafka";
    }

    public record Stats(
            String model, String groupId, int port, long received, SortedMap<Integer, Long> byPartition) {
    }

    @GetMapping("/stats")
    public Stats stats() {
        return new Stats(model, groupId, port, counters.receivedCount(), counters.countsByPartition());
    }

    @GetMapping("/stats/reset")
    public Map<String, String> reset() {
        counters.reset();
        return Map.of("status", "compteurs remis a zero");
    }
}

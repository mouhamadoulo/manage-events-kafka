package com.formation.eda.notification.application;

import com.formation.eda.events.order.OrderPlaced;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * L'effet metier de notification-service, en un seul endroit.
 *
 * <p>WHY un handler separe des listeners : les <b>trois</b> listeners — topic principal,
 * {@code .retry-1}, {@code .retry-2} — doivent produire exactement le meme effet. Dupliquer le
 * traitement dans chacun serait la facon la plus sure de les faire diverger, et une divergence
 * entre un etage de retry et le topic principal ne se verrait qu'en panne, c'est-a-dire au pire
 * moment.
 *
 * <p>Les listeners restent dans ce service plutot que dans {@code common-kafka} : trois
 * methodes de quatre lignes, contre une abstraction de handler generique dont on ne connaitra
 * la bonne forme qu'en phase 6, avec 5 services et 2 etages chacun. A revoir en P6-T1.
 */
@Component
public class OrderNotificationHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderNotificationHandler.class);

    private final EventCounters counters;
    private final FaultInjection faultInjection;

    public OrderNotificationHandler(EventCounters counters, FaultInjection faultInjection) {
        this.counters = counters;
        this.faultInjection = faultInjection;
    }

    /**
     * @throws com.formation.eda.kafka.reliability.TransientProcessingException panne injectee,
     *         profil {@code lab} uniquement
     * @throws com.formation.eda.kafka.reliability.NonRetryableEventException panne definitive
     *         injectee, profil {@code lab} uniquement
     */
    public void handle(ConsumerRecord<String, OrderPlaced> record) {
        OrderPlaced event = record.value();

        // Valeur nulle : tombstone (legitime sur un topic compacte, aberrant ici) ou echec de
        // deserialisation passe par ErrorHandlingDeserializer. Sans ce garde-fou c'est une NPE,
        // donc un echec de traitement, donc un trajet complet par les etages de retry pour un
        // message qui n'en contient rien.
        if (event == null || event.envelope() == null) {
            log.warn("ignore un message sans payload exploitable : cle={} topic={} partition={} offset={}",
                    record.key(), record.topic(), record.partition(), record.offset());
            return;
        }

        // AVANT l'effet : une panne injectee doit empecher l'effet, pas s'y ajouter.
        faultInjection.check(record.headers(), event.envelope().eventId());

        long rank = counters.record(record.partition());

        // Ecart entre l'instant du fait metier et l'instant de reception : la latence reelle de
        // la chaine, bout en bout. Sur un record venu d'un etage de retry, elle inclut le delai
        // de l'etage — c'est voulu, c'est ce que le client a reellement attendu.
        Duration lag = Duration.between(event.envelope().occurredAt(), Instant.now());

        log.info("traite #{} {} cle={} topic={} partition={} offset={} latence={} ms client={} total={} {}",
                rank, event.envelope().eventType(), record.key(), record.topic(),
                record.partition(), record.offset(), lag.toMillis(),
                event.customerId(), event.totalAmount(), event.currency());
    }
}

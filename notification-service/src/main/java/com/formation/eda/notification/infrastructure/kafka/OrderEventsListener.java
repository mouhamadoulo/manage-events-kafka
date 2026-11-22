package com.formation.eda.notification.infrastructure.kafka;

import com.formation.eda.events.Topics;
import com.formation.eda.events.order.OrderPlaced;
import com.formation.eda.notification.application.EventCounters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

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
 *
 * <p>Desactive sous le profil {@code cloud-stream}, ou la meme consommation est assuree par
 * le binding fonctionnel (P1-T7). Les deux ensemble liraient le topic dans deux groupes
 * distincts et compteraient chaque evenement deux fois.
 */
@Component
@Profile("!cloud-stream")
public class OrderEventsListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventsListener.class);

    private final EventCounters counters;

    public OrderEventsListener(EventCounters counters) {
        this.counters = counters;
    }

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

        // Une valeur nulle est un tombstone : legitime sur un topic compacte (l'event store
        // de la phase 8 en recevra), aberrant sur un topic d'evenements. Sans ce garde-fou,
        // c'est une NPE dans le listener — donc un echec de traitement, donc un repoll du
        // meme message a l'infini une fois le retry en place.
        if (event == null || event.envelope() == null) {
            log.warn("ignore un message sans payload exploitable : cle={} partition={} offset={}",
                    record.key(), record.partition(), record.offset());
            return;
        }

        long rank = counters.record(record.partition());

        // Ecart entre l'instant du fait metier et l'instant de reception : la latence reelle
        // de la chaine, bout en bout. Le timestamp du record, lui, est l'instant de
        // publication cote broker.
        Duration lag = Duration.between(event.envelope().occurredAt(), Instant.now());

        log.info("recu #{} {} cle={} partition={} offset={} latence={} ms client={} total={} {}",
                rank,
                event.envelope().eventType(),
                record.key(),
                record.partition(),
                record.offset(),
                lag.toMillis(),
                event.customerId(),
                event.totalAmount(),
                event.currency());
    }
}

package com.formation.eda.notification.infrastructure.kafka;

import com.formation.eda.events.Topics;
import com.formation.eda.events.order.OrderPlaced;
import com.formation.eda.kafka.reliability.ConsumerReliabilityProperties;
import com.formation.eda.kafka.reliability.RetryStageSupport;
import com.formation.eda.notification.application.OrderNotificationHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumers des etages de retry.
 *
 * <p>Chaque etage appelle le MEME handler que le topic principal : un etage de retry n'est pas
 * un traitement degrade, c'est le meme traitement plus tard.
 *
 * <p>La seule chose que ces methodes ajoutent est {@link RetryStageSupport#requireDue} : le
 * delai de l'etage se compte depuis la publication d'origine, et un record arrive trop tot
 * doit <b>lever</b> pour que sa partition soit mise en pause (cf. {@code NotYetDueException} —
 * sortir normalement perdrait le message en silence).
 *
 * <p>WHY deux methodes et non une boucle sur les etages : {@code @KafkaListener} exige des
 * noms de topics constants a la compilation. Deux etages, deux methodes. S'il en fallait dix,
 * la question se poserait autrement — et c'est ce que la phase 6 tranchera, avec 5 services.
 */
@Component
@Profile("!cloud-stream")
public class OrderRetryListener {

    private static final Logger log = LoggerFactory.getLogger(OrderRetryListener.class);

    /**
     * Nombre d'etages reellement ecoutes par cette classe. Doit rester egal au nombre de
     * methodes {@code @KafkaListener} ci-dessous.
     *
     * <p>C'est le pendant de {@code eda.reliability.stage-delays} : les delais sont reglables,
     * les listeners non. La verification est faite ici, au demarrage, parce que c'est ici que
     * vit la verite sur le nombre d'etages ecoutes.
     */
    static final int LISTENED_STAGES = 2;

    private final OrderNotificationHandler handler;
    private final RetryStageSupport retryStageSupport;

    public OrderRetryListener(OrderNotificationHandler handler,
                              RetryStageSupport retryStageSupport,
                              ConsumerReliabilityProperties properties) {
        // Echoue au DEMARRAGE si la configuration declare un nombre d'etages different de
        // celui qu'on ecoute. Un etage configure sans listener bloque sa partition (publication
        // vers un topic inexistant, rejouee en boucle) ; un listener sans delai configure
        // envoie en DLT sans attendre. Les deux sont silencieux a l'execution.
        properties.requireStageCount(LISTENED_STAGES);
        this.handler = handler;
        this.retryStageSupport = retryStageSupport;
    }

    @KafkaListener(
            topics = Topics.ORDER_EVENTS_RETRY_1,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "retryContainerFactory")
    public void onFirstRetryStage(ConsumerRecord<String, OrderPlaced> record) {
        process(record);
    }

    @KafkaListener(
            topics = Topics.ORDER_EVENTS_RETRY_2,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "retryContainerFactory")
    public void onSecondRetryStage(ConsumerRecord<String, OrderPlaced> record) {
        process(record);
    }

    private void process(ConsumerRecord<String, OrderPlaced> record) {
        retryStageSupport.requireDue(record);
        log.info("reessai depuis {} partition={} offset={}",
                record.topic(), record.partition(), record.offset());
        handler.handle(record);
    }
}

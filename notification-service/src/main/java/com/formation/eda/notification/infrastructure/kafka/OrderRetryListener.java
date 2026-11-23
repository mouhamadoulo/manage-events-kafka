package com.formation.eda.notification.infrastructure.kafka;

import com.formation.eda.events.Topics;
import com.formation.eda.events.order.OrderPlaced;
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

    private final OrderNotificationHandler handler;
    private final RetryStageSupport retryStageSupport;

    public OrderRetryListener(OrderNotificationHandler handler, RetryStageSupport retryStageSupport) {
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

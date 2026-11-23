package com.formation.eda.notification.infrastructure.kafka;

import com.formation.eda.events.Topics;
import com.formation.eda.events.order.OrderPlaced;
import com.formation.eda.notification.application.OrderNotificationHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumer terminal : notification-service lit les evenements de commande et n'en produit
 * aucun. Rien dans le systeme ne depend de lui — l'arreter ne casse pas la saga, les messages
 * s'accumulent et il rattrape a son redemarrage. C'est la demonstration la plus simple du
 * decouplage.
 *
 * <p>Depuis la phase 4, ce listener ne fait plus que router : l'effet metier est dans
 * {@link OrderNotificationHandler}, partage avec les deux listeners d'etage de retry
 * ({@link OrderRetryListener}). Trois chemins, un seul traitement.
 *
 * <p>La faille laissee volontairement en phase 1 — un message illisible bloquait la partition
 * pour toujours — est fermee : {@code ErrorHandlingDeserializer} transforme un echec de
 * desserialisation en record a valeur nulle, et {@code mainContainerFactory} y ajoute
 * l'idempotence, le retry etage et la DLT.
 *
 * <p>Desactive sous le profil {@code cloud-stream}, ou la meme consommation est assuree par le
 * binding fonctionnel (P1-T7). Ce profil reste volontairement <b>nu</b>, sans aucune mecanique
 * de fiabilite : c'est le temoin de l'experience P4-T6.
 */
@Component
@Profile("!cloud-stream")
public class OrderEventsListener {

    private final OrderNotificationHandler handler;

    public OrderEventsListener(OrderNotificationHandler handler) {
        this.handler = handler;
    }

    /**
     * {@code groupId} vient de la configuration, pas d'une constante.
     *
     * <p>WHY: le groupe est ce qui determine le partage du travail. Deux instances avec le meme
     * groupe se partagent les partitions ; avec deux groupes differents, chacune recoit
     * <b>tout</b>. Pouvoir le changer par variable d'environnement permet de faire les deux
     * experiences (P1-T5) sans recompiler.
     *
     * <p>WHY {@code containerFactory} explicite : la fabrique par defaut de Boot n'a ni
     * intercepteur d'idempotence, ni gestionnaire d'erreur. Nommer la fabrique ici est ce qui
     * rend visible, a la lecture du listener, qu'il est protege.
     */
    @KafkaListener(
            topics = Topics.ORDER_EVENTS,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "mainContainerFactory")
    public void onOrderEvent(ConsumerRecord<String, OrderPlaced> record) {
        handler.handle(record);
    }
}

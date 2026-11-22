package com.formation.eda.notification.infrastructure.stream;

import com.formation.eda.events.order.OrderPlaced;
import com.formation.eda.notification.application.EventCounters;
import com.formation.eda.notification.infrastructure.kafka.OrderEventsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.Message;
import org.springframework.kafka.support.KafkaHeaders;

import java.util.function.Consumer;

/**
 * Le MEME cas que {@link OrderEventsListener}, ecrit en Spring Cloud Stream 5.1.0 (P1-T7).
 *
 * <p>Actif uniquement sous le profil {@code cloud-stream}, qui desactive en meme temps le
 * {@code @KafkaListener}. Les deux ne doivent pas tourner ensemble : ils liraient le meme
 * topic dans deux groupes differents et chaque evenement serait compte deux fois.
 *
 * <p>Differences de fond, pas de syntaxe :
 * <ul>
 *   <li><b>pas d'annotation Kafka.</b> Le bean est un {@link Consumer}, du {@code java.util}
 *       pur. Le binder est choisi par la dependance presente au classpath, pas par le code :
 *       le meme bean pourrait etre branche sur RabbitMQ sans etre modifie. C'est le but, et
 *       c'est aussi la limite — ce qui est portable est l'intersection des brokers ;</li>
 *   <li><b>conversion differente.</b> Ici le payload est converti par les
 *       {@code MessageConverter} de Spring Messaging, a partir du {@code contentType}.
 *       En spring-kafka, c'est un {@code Deserializer} Kafka. Deux chaines de conversion
 *       distinctes, deux endroits ou regler les problemes de type ;</li>
 *   <li><b>les specificites Kafka sont dans la configuration</b> ({@code spring.cloud.stream
 *       .kafka.binder}) et non dans le code. Pratique tant qu'on reste dans ce que le binder
 *       expose ; a contourner des qu'on en sort.</li>
 * </ul>
 *
 * <p>{@code @StreamListener} et {@code @EnableBinding} n'existent plus depuis Spring Cloud
 * Stream 4 : seul le modele fonctionnel subsiste (cf. CLAUDE.md §5).
 */
@Configuration
@Profile("cloud-stream")
public class CloudStreamOrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(CloudStreamOrderConsumer.class);

    private final EventCounters counters;

    public CloudStreamOrderConsumer(EventCounters counters) {
        this.counters = counters;
    }

    /**
     * Le nom du bean est le nom de la fonction : il determine le nom des bindings
     * ({@code onOrderPlaced-in-0}) dans la configuration. Renommer la methode renomme la
     * configuration — couplage discret, a connaitre.
     *
     * <p>On recoit un {@link Message} plutot que le payload nu pour pouvoir lire la partition
     * et l'offset dans les headers. Avec {@code Consumer<OrderPlaced>}, ces informations
     * n'existent plus dans la signature : le modele est plus simple parce qu'il cache le
     * transport.
     */
    @Bean
    public Consumer<Message<OrderPlaced>> onOrderPlaced() {
        return message -> {
            OrderPlaced event = message.getPayload();
            Integer partition = message.getHeaders()
                    .get(KafkaHeaders.RECEIVED_PARTITION, Integer.class);
            Long offset = message.getHeaders().get(KafkaHeaders.OFFSET, Long.class);

            counters.record(partition == null ? -1 : partition);

            log.info("[cloud-stream] recu {} cle={} partition={} offset={} total={} {}",
                    event.envelope().eventType(),
                    message.getHeaders().get(KafkaHeaders.RECEIVED_KEY),
                    partition,
                    offset,
                    event.totalAmount(),
                    event.currency());
        };
    }
}

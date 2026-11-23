package com.formation.eda.kafka.reliability;

import org.springframework.kafka.listener.BackOffHandler;
import org.springframework.kafka.listener.DefaultBackOffHandler;
import org.springframework.kafka.listener.ListenerContainerPauseService;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.time.Duration;

/**
 * Applique un delai en mettant en pause <b>la seule partition</b> du record concerne.
 *
 * <p>WHY cette classe alors que spring-kafka fournit {@code ContainerPausingBackOffHandler} :
 * celui-ci appelle {@code pauseService.pause(container, duree)}, qui suspend <b>toutes</b> les
 * partitions assignees au consumer — y compris celles dont les records sont deja echus. Sur un
 * {@code .retry-1} a 3 partitions, un seul record arrive trop tot retarde donc les deux autres
 * partitions de 5 secondes. Mesure et verifie par test : le handler du framework n'a aucun
 * moyen de faire mieux, parce que {@link BackOffHandler#onNextBackOff(MessageListenerContainer,
 * Exception, long)} ne recoit <b>pas le record</b>.
 *
 * <p>D'ou le detour : {@link NotYetDueException} porte sa {@code TopicPartition}, et ce handler
 * la lit. C'est le seul vehicule disponible.
 *
 * <p>L'offset n'est pas commite, et le gestionnaire d'erreur a deja repositionne le consumer
 * sur le record ({@code seekAfterError}, actif par defaut) : a la reprise, le record est
 * redelivre, il est alors echu, et il passe.
 */
public class PartitionPausingBackOffHandler implements BackOffHandler {

    private final ListenerContainerPauseService pauseService;

    /**
     * Repli pour tout delai qui n'est pas une attente d'echeance : le handler par defaut de
     * spring-kafka, qui fait dormir le thread du consumer. Acceptable la, parce que les delais
     * concernes sont les reessais bloquants de quelques centaines de millisecondes.
     */
    private final BackOffHandler threadSleeping = new DefaultBackOffHandler();

    public PartitionPausingBackOffHandler(ListenerContainerPauseService pauseService) {
        this.pauseService = pauseService;
    }

    @Override
    public void onNextBackOff(MessageListenerContainer container, Exception exception, long interval) {
        if (ConsumerReliabilityConfiguration.rootNotYetDue(exception) instanceof NotYetDueException notYetDue
                && notYetDue.topicPartition() != null) {
            // pausePartition programme aussi la reprise a l'echeance, via le TaskScheduler
            // confie au ListenerContainerPauseService. Aucun thread ne dort.
            pauseService.pausePartition(container, notYetDue.topicPartition(), Duration.ofMillis(interval));
            return;
        }
        threadSleeping.onNextBackOff(container, exception, interval);
    }

    @Override
    public void onNextBackOff(MessageListenerContainer container,
                              org.apache.kafka.common.TopicPartition partition, long interval) {
        pauseService.pausePartition(container, partition, Duration.ofMillis(interval));
    }
}

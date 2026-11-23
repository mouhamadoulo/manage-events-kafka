package com.formation.eda.kafka.reliability;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerContainerPauseService;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.ExponentialBackOff;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Le classement des exceptions est la decision la plus consequente de la phase 4 : une
 * exception mal classee produit soit un message perdu (non-retryable reessaye indefiniment,
 * puis abandonne), soit une partition qui tourne en rond sur une erreur qui ne se reparera
 * jamais. Ces tests pilotent le {@link DefaultErrorHandler} sans broker.
 */
class ErrorClassificationTest {

    /** Delais raccourcis : ces tests dorment vraiment pendant le backoff bloquant. */
    private static final ConsumerReliabilityProperties PROPS = new ConsumerReliabilityProperties(
            List.of(Duration.ofSeconds(5), Duration.ofSeconds(30)),
            2,
            Duration.ofMillis(10),
            2.0,
            Duration.ofMillis(5));

    private List<ConsumerRecord<?, ?>> recovered;
    private DefaultErrorHandler handler;
    private Consumer<?, ?> consumer;
    private MessageListenerContainer container;

    @BeforeEach
    void setUp() {
        recovered = new ArrayList<>();
        handler = ConsumerReliabilityConfiguration.blockingErrorHandler(
                (record, exception) -> recovered.add(record), PROPS);
        consumer = mock(Consumer.class);
        container = mock(MessageListenerContainer.class);
    }

    @Test
    @DisplayName("une exception non retryable part directement au recoverer")
    void nonRetryableExceptionGoesStraightToTheRecoverer() {
        boolean wasRecovered = handle(failure(new DeserializationException(
                "octet magique inconnu", new byte[]{1, 2, 3}, false, null)));

        // Un message qui ne se decode pas ne se decodera pas mieux dans 400 ms.
        assertThat(wasRecovered).isTrue();
        assertThat(recovered).hasSize(1);
    }

    @Test
    @DisplayName("une exception transitoire est reessayee deux fois puis recuperee")
    void transientExceptionIsRetriedTwiceThenRecovered() {
        Exception thrown = failure(new TransientProcessingException("base injoignable", null));

        assertThat(handle(thrown)).isFalse();
        assertThat(handle(thrown)).isFalse();
        assertThat(recovered).isEmpty();

        // Troisieme passage : le backoff est epuise (1 tentative + 2 retries).
        assertThat(handle(thrown)).isTrue();
        assertThat(recovered).hasSize(1);
    }

    @Test
    @DisplayName("NonRetryableEventException est classee non retryable")
    void nonRetryableEventExceptionIsClassifiedAsSuch() {
        boolean wasRecovered = handle(failure(new NonRetryableEventException("commande inconnue")));

        assertThat(wasRecovered).isTrue();
        assertThat(recovered).hasSize(1);
    }

    @Test
    @DisplayName("le backoff bloquant porte du jitter")
    void blockingBackOffCarriesJitter() {
        ExponentialBackOff backOff = ConsumerReliabilityConfiguration.blockingBackOff(PROPS);

        // WHY ce test : sans jitter, N instances du meme groupe reessaient a la meme
        // milliseconde et frappent ensemble la dependance qui vient de tomber.
        assertThat(backOff.getJitter()).isEqualTo(5L);
        assertThat(backOff.getInitialInterval()).isEqualTo(10L);
        assertThat(backOff.getMultiplier()).isEqualTo(2.0);
        assertThat(backOff.getMaxAttempts()).isEqualTo(2L);
    }

    @Test
    @DisplayName("sur un etage de retry, un record non echu met sa partition en pause sans etre recupere")
    void anUndueRecordPausesItsPartitionInsteadOfBeingRecovered() {
        ListenerContainerPauseService pauseService = mock(ListenerContainerPauseService.class);
        DefaultErrorHandler retryHandler = new ConsumerReliabilityConfiguration()
                .retryErrorHandler((record, exception) -> recovered.add(record), pauseService);

        // L'exception arrive EMBALLEE par le conteneur : sans deballage, le differe ne serait
        // pas reconnu et le record partirait a l'etage suivant immediatement.
        boolean wasRecovered = retryHandler.handleOne(
                failure(new NotYetDueException(4000, RETRY_PARTITION)),
                RETRY_RECORD, consumer, container);

        assertThat(wasRecovered).isFalse();
        assertThat(recovered).isEmpty();
        verify(pauseService).pausePartition(container, RETRY_PARTITION, Duration.ofMillis(4000));
        // Et surtout PAS pause(container) : le handler de spring-kafka
        // (ContainerPausingBackOffHandler) suspendrait toutes les partitions assignees, dont
        // celles dont les records sont deja echus.
        verify(pauseService, never()).pause(any(MessageListenerContainer.class), any(Duration.class));
    }

    @Test
    @DisplayName("sans partition connue, l'attente retombe sur le handler qui fait dormir le thread")
    void withoutAKnownPartitionTheWaitFallsBackToThreadSleeping() {
        ListenerContainerPauseService pauseService = mock(ListenerContainerPauseService.class);
        PartitionPausingBackOffHandler handler = new PartitionPausingBackOffHandler(pauseService);

        // DefaultBackOffHandler dort par tranches tant que le conteneur tourne : sans ce stub,
        // il rendrait la main immediatement et le test ne mesurerait rien.
        when(container.isRunning()).thenReturn(true);

        long before = System.currentTimeMillis();
        handler.onNextBackOff(container, failure(new NotYetDueException(30)), 30L);

        assertThat(System.currentTimeMillis() - before).isGreaterThanOrEqualTo(25L);
        verifyNoInteractions(pauseService);
    }

    @Test
    @DisplayName("sur un etage de retry, un echec reel part immediatement a la destination suivante")
    void arealFailureOnARetryStageGoesStraightToTheNextDestination() {
        ListenerContainerPauseService pauseService = mock(ListenerContainerPauseService.class);
        DefaultErrorHandler retryHandler = new ConsumerReliabilityConfiguration()
                .retryErrorHandler((record, exception) -> recovered.add(record), pauseService);

        // Zero reessai bloquant sur un etage : le delai de l'etage EST l'attente.
        boolean wasRecovered = retryHandler.handleOne(
                failure(new TransientProcessingException("toujours injoignable", null)),
                RETRY_RECORD, consumer, container);

        assertThat(wasRecovered).isTrue();
        assertThat(recovered).hasSize(1);
        verifyNoInteractions(pauseService);
    }

    @Test
    @DisplayName("rootNotYetDue traverse les causes et ignore les autres exceptions")
    void rootNotYetDueWalksTheCauseChain() {
        NotYetDueException notYetDue = new NotYetDueException(1);

        assertThat(ConsumerReliabilityConfiguration.rootNotYetDue(notYetDue)).isSameAs(notYetDue);
        assertThat(ConsumerReliabilityConfiguration.rootNotYetDue(
                new IllegalStateException("enveloppe", notYetDue))).isSameAs(notYetDue);
        assertThat(ConsumerReliabilityConfiguration.rootNotYetDue(
                new IllegalStateException("sans differe"))).isNull();
    }

    /** Le conteneur enveloppe toujours l'exception du listener. */
    private static Exception failure(Exception cause) {
        return new ListenerExecutionFailedException("echec du listener", cause);
    }

    private boolean handle(Exception thrown) {
        return handler.handleOne(thrown, RECORD, consumer, container);
    }

    private static final ConsumerRecord<String, Object> RECORD =
            new ConsumerRecord<>("shop.order.events.v1", 0, 42L, "order-1", "payload");

    private static final ConsumerRecord<String, Object> RETRY_RECORD =
            new ConsumerRecord<>("shop.order.events.v1.retry-1", 0, 7L, "order-1", "payload");

    private static final TopicPartition RETRY_PARTITION =
            new TopicPartition("shop.order.events.v1.retry-1", 0);
}

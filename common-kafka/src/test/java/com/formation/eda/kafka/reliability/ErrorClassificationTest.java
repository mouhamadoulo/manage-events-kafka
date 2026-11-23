package com.formation.eda.kafka.reliability;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.ExponentialBackOff;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

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

    /** Le conteneur enveloppe toujours l'exception du listener. */
    private static Exception failure(Exception cause) {
        return new ListenerExecutionFailedException("echec du listener", cause);
    }

    private boolean handle(Exception thrown) {
        return handler.handleOne(thrown, RECORD, consumer, container);
    }

    private static final ConsumerRecord<String, Object> RECORD =
            new ConsumerRecord<>("shop.order.events.v1", 0, 42L, "order-1", "payload");
}

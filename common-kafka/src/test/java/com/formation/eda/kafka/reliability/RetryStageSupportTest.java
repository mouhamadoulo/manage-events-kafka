package com.formation.eda.kafka.reliability;

import com.formation.eda.events.EventHeaders;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Le differe d'un etage de retry. Le point delicat n'est pas le calcul de l'echeance, c'est le
 * fait qu'il doive se signaler par une EXCEPTION : un listener qui sortirait normalement sans
 * avoir travaille laisserait la marque d'idempotence en place, et au reveil l'evenement serait
 * classe doublon — travail perdu en silence.
 */
class RetryStageSupportTest {

    private static final String MAIN = "shop.order.events.v1";
    private static final Instant NOW = Instant.parse("2026-11-23T12:00:00Z");

    private static final ConsumerReliabilityProperties PROPS = new ConsumerReliabilityProperties(
            List.of(Duration.ofSeconds(5), Duration.ofSeconds(30)),
            2, Duration.ofMillis(200), 2.0, Duration.ofMillis(50));

    private final RetryStageSupport support =
            new RetryStageSupport(PROPS, Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    @DisplayName("un record echu passe")
    void aDueRecordPasses() {
        ConsumerRecord<String, Object> record =
                record(MAIN + ".retry-1", NOW.minusSeconds(6));

        assertThatCode(() -> support.requireDue(record)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("un record non echu leve avec la duree restante")
    void anUndueRecordThrowsWithTheRemainingDelay() {
        ConsumerRecord<String, Object> record =
                record(MAIN + ".retry-1", NOW.minusSeconds(1));

        assertThatThrownBy(() -> support.requireDue(record))
                .isInstanceOf(NotYetDueException.class)
                .extracting(e -> ((NotYetDueException) e).remainingMillis())
                .isEqualTo(4000L);

        // La partition voyage dans l'exception : le BackOffHandler ne recoit pas le record.
        assertThatThrownBy(() -> support.requireDue(record))
                .extracting(e -> ((NotYetDueException) e).topicPartition())
                .isEqualTo(new org.apache.kafka.common.TopicPartition(MAIN + ".retry-1", 0));
    }

    @Test
    @DisplayName("le second etage attend son propre delai")
    void theSecondStageWaitsItsOwnDelay() {
        ConsumerRecord<String, Object> record =
                record(MAIN + ".retry-2", NOW.minusSeconds(10));

        assertThatThrownBy(() -> support.requireDue(record))
                .isInstanceOf(NotYetDueException.class)
                .extracting(e -> ((NotYetDueException) e).remainingMillis())
                .isEqualTo(20_000L);
    }

    @Test
    @DisplayName("un record sans x-origin-timestamp est traite immediatement")
    void aRecordWithoutOriginTimestampIsProcessedImmediately() {
        // Cas d'un message publie a la main par un operateur. Une echeance incalculable ne doit
        // pas mettre le record en attente indefinie : mieux vaut le traiter tout de suite.
        ConsumerRecord<String, Object> record = record(MAIN + ".retry-1", null);

        assertThatCode(() -> support.requireDue(record)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("un record du topic principal n'a aucune echeance")
    void aRecordFromTheMainTopicHasNoDeadline() {
        ConsumerRecord<String, Object> record = record(MAIN, NOW);

        assertThatCode(() -> support.requireDue(record)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("un etage sans delai declare est refuse")
    void aStageWithoutADeclaredDelayIsRejected() {
        // Trois topics de retry pour deux delais configures : echouer vaut mieux que calculer
        // une echeance sur un delai invente.
        ConsumerRecord<String, Object> record = record(MAIN + ".retry-3", NOW);

        assertThatThrownBy(() -> support.requireDue(record))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("3");
    }

    @Test
    @DisplayName("l'echeance se compte depuis la publication d'origine, pas depuis l'arrivee")
    void theDeadlineIsCountedFromTheOriginalPublication() {
        // Le timestamp du RECORD est recent (republie a l'instant dans l'etage), mais
        // x-origin-timestamp est vieux de 6 s : le record est du.
        // WHY ce test : un delai compte a l'arrivee repartirait de zero a chaque redemarrage du
        // consumer, et un message pourrait attendre indefiniment.
        Headers headers = new RecordHeaders();
        headers.add(EventHeaders.ORIGIN_TIMESTAMP,
                String.valueOf(NOW.minusSeconds(6).toEpochMilli()).getBytes(StandardCharsets.UTF_8));
        ConsumerRecord<String, Object> record = new ConsumerRecord<>(
                MAIN + ".retry-1", 0, 0L, NOW.toEpochMilli(), TimestampType.CREATE_TIME, -1, -1,
                "order-1", "payload", headers, Optional.empty());

        assertThatCode(() -> support.requireDue(record)).doesNotThrowAnyException();
    }

    private static ConsumerRecord<String, Object> record(String topic, Instant originTimestamp) {
        Headers headers = new RecordHeaders();
        if (originTimestamp != null) {
            headers.add(EventHeaders.ORIGIN_TIMESTAMP,
                    String.valueOf(originTimestamp.toEpochMilli()).getBytes(StandardCharsets.UTF_8));
        }
        return new ConsumerRecord<>(
                topic, 0, 0L, NOW.toEpochMilli(), TimestampType.CREATE_TIME, -1, -1,
                "order-1", "payload", headers, Optional.empty());
    }

    @Test
    @DisplayName("NotYetDueException ne porte jamais une duree negative")
    void notYetDueExceptionNeverCarriesANegativeDelay() {
        assertThat(new NotYetDueException(0).remainingMillis()).isZero();
        assertThatThrownBy(() -> new NotYetDueException(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

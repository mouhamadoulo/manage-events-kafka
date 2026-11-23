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
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Un message en DLT qu'on ne sait pas rejouer n'est pas une DLT, c'est une poubelle. Ces
 * headers sont tout ce qu'un operateur aura pour decider quoi faire — et tout ce que l'endpoint
 * de rejeu aura pour savoir ou republier.
 */
class DiagnosticHeadersTest {

    private static final String MAIN = "shop.order.events.v1";
    private static final Instant NOW = Instant.parse("2026-11-23T12:00:00Z");
    private static final Instant PUBLISHED_AT = Instant.parse("2026-11-23T11:59:50Z");

    private final DiagnosticHeaders diagnosticHeaders =
            new DiagnosticHeaders(Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    @DisplayName("x-origin-topic est pose depuis le topic principal")
    void originTopicIsSetFromTheMainTopic() {
        Headers added = diagnosticHeaders.apply(record(MAIN, new RecordHeaders()), boom());

        assertThat(value(added, EventHeaders.ORIGIN_TOPIC)).isEqualTo(MAIN);
    }

    @Test
    @DisplayName("x-origin-topic n'est pas repose s'il existe deja")
    void originTopicIsNotSetTwice() {
        Headers existing = new RecordHeaders();
        existing.add(EventHeaders.ORIGIN_TOPIC, MAIN.getBytes(StandardCharsets.UTF_8));

        Headers added = diagnosticHeaders.apply(record(MAIN + ".retry-2", existing), boom());

        // WHY: les headers kafka_dlt-original-* du framework sont reecrits a chaque etage. Au
        // bout de deux etages ils designent .retry-1, pas le topic metier. Le rejeu a besoin du
        // point de depart, donc ce header doit etre pose une seule fois.
        assertThat(added.lastHeader(EventHeaders.ORIGIN_TOPIC)).isNull();
    }

    @Test
    @DisplayName("x-origin-timestamp est pose depuis le timestamp du record, une seule fois")
    void originTimestampIsSetFromTheRecordTimestampOnce() {
        Headers added = diagnosticHeaders.apply(record(MAIN, new RecordHeaders()), boom());

        assertThat(value(added, EventHeaders.ORIGIN_TIMESTAMP))
                .isEqualTo(String.valueOf(PUBLISHED_AT.toEpochMilli()));

        Headers existing = new RecordHeaders();
        existing.add(EventHeaders.ORIGIN_TIMESTAMP, "123".getBytes(StandardCharsets.UTF_8));
        Headers secondPass = diagnosticHeaders.apply(record(MAIN + ".retry-1", existing), boom());

        // Sinon l'echeance du second etage repartirait de l'arrivee dans le premier, et un
        // message pourrait attendre indefiniment en traversant les etages.
        assertThat(secondPass.lastHeader(EventHeaders.ORIGIN_TIMESTAMP)).isNull();
    }

    @Test
    @DisplayName("x-retry-stage porte l'etage d'ou vient le record")
    void retryStageCarriesTheStageTheRecordCameFrom() {
        assertThat(value(diagnosticHeaders.apply(record(MAIN, new RecordHeaders()), boom()),
                EventHeaders.RETRY_STAGE)).isEqualTo("0");
        assertThat(value(diagnosticHeaders.apply(record(MAIN + ".retry-1", new RecordHeaders()), boom()),
                EventHeaders.RETRY_STAGE)).isEqualTo("1");
        assertThat(value(diagnosticHeaders.apply(record(MAIN + ".retry-2", new RecordHeaders()), boom()),
                EventHeaders.RETRY_STAGE)).isEqualTo("2");
    }

    @Test
    @DisplayName("x-failed-at porte l'instant de la mise en echec")
    void failedAtCarriesTheMomentOfFailure() {
        Headers added = diagnosticHeaders.apply(record(MAIN, new RecordHeaders()), boom());

        assertThat(value(added, EventHeaders.FAILED_AT)).isEqualTo(NOW.toString());
    }

    private static Exception boom() {
        return new TransientProcessingException("base injoignable", null);
    }

    private static String value(Headers headers, String name) {
        return HeaderReader.string(headers, name);
    }

    private static ConsumerRecord<String, Object> record(String topic, Headers headers) {
        return new ConsumerRecord<>(
                topic, 0, 0L, PUBLISHED_AT.toEpochMilli(), TimestampType.CREATE_TIME, -1, -1,
                "order-1", "payload", headers, Optional.empty());
    }
}

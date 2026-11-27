package com.formation.eda.notification.application;

import com.formation.eda.events.EventHeaders;
import com.formation.eda.events.order.OrderLine;
import com.formation.eda.events.order.OrderPlaced;
import com.formation.eda.kafka.reliability.NonRetryableEventException;
import com.formation.eda.kafka.reliability.TransientProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * L'effet metier, et l'injection de panne qui permet de le faire echouer a la demande.
 *
 * <p>Sans injection de panne, la phase 4 serait invérifiable sur ce service : son effet est un
 * compteur en memoire, donc rien ne peut echouer, donc ni les etages de retry, ni la branche
 * « erreur metier » de la DLT, ni le rejeu n'auraient de declencheur.
 */
class OrderNotificationHandlerTest {

    private static final String TOPIC = "shop.order.events.v1";

    private EventCounters counters;

    @BeforeEach
    void setUp() {
        counters = new EventCounters();
    }

    @Test
    @DisplayName("un evenement valide incremente le compteur")
    void aValidEventIncrementsTheCounter() {
        OrderNotificationHandler handler = new OrderNotificationHandler(counters, FaultInjection.DISABLED);

        handler.handle(record(orderPlaced(), new RecordHeaders()));

        assertThat(counters.receivedCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("un record a valeur nulle est ignore sans lever")
    void aTombstoneIsIgnoredWithoutThrowing() {
        OrderNotificationHandler handler = new OrderNotificationHandler(counters, FaultInjection.DISABLED);

        // Un tombstone est legitime sur un topic compacte (l'event store de la phase 8 en
        // recevra), aberrant ici. Sans ce garde-fou c'est une NPE, donc un echec de
        // traitement, donc un trajet complet par les etages de retry pour rien.
        assertThatCode(() -> handler.handle(record(null, new RecordHeaders())))
                .doesNotThrowAnyException();
        assertThat(counters.receivedCount()).isZero();
    }

    @Test
    @DisplayName("x-fail-times a 2 leve deux fois puis reussit")
    void failTimesTwoThrowsTwiceThenSucceeds() {
        OrderNotificationHandler handler = new OrderNotificationHandler(counters, new LabFaultInjection());
        OrderPlaced event = orderPlaced();
        ConsumerRecord<String, OrderPlaced> record = record(event, headers(EventHeaders.FAIL_TIMES, "2"));

        assertThatThrownBy(() -> handler.handle(record)).isInstanceOf(TransientProcessingException.class);
        assertThatThrownBy(() -> handler.handle(record)).isInstanceOf(TransientProcessingException.class);
        assertThatCode(() -> handler.handle(record)).doesNotThrowAnyException();

        assertThat(counters.receivedCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("le compte d'echecs est tenu par eventId, pas globalement")
    void theFailureCountIsKeptPerEventId() {
        OrderNotificationHandler handler = new OrderNotificationHandler(counters, new LabFaultInjection());
        ConsumerRecord<String, OrderPlaced> first = record(orderPlaced(), headers(EventHeaders.FAIL_TIMES, "1"));
        ConsumerRecord<String, OrderPlaced> second = record(orderPlaced(), headers(EventHeaders.FAIL_TIMES, "1"));

        assertThatThrownBy(() -> handler.handle(first)).isInstanceOf(TransientProcessingException.class);
        // Un compteur global ferait reussir le second du premier coup, et l'experience des
        // etages ne mesurerait rien.
        assertThatThrownBy(() -> handler.handle(second)).isInstanceOf(TransientProcessingException.class);
        assertThatCode(() -> handler.handle(first)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("x-fail-fatal leve une exception non retryable")
    void failFatalThrowsANonRetryableException() {
        OrderNotificationHandler handler = new OrderNotificationHandler(counters, new LabFaultInjection());

        assertThatThrownBy(() -> handler.handle(record(orderPlaced(), headers(EventHeaders.FAIL_FATAL, "true"))))
                .isInstanceOf(NonRetryableEventException.class);
        assertThat(counters.receivedCount()).isZero();
    }

    @Test
    @DisplayName("les headers d'injection sont ignores hors profil lab")
    void injectionHeadersAreIgnoredOutsideTheLabProfile() {
        OrderNotificationHandler handler = new OrderNotificationHandler(counters, FaultInjection.DISABLED);

        // Une panne ne doit pas pouvoir etre commandee depuis un message en fonctionnement
        // normal : le header vient du producteur, donc de l'exterieur.
        assertThatCode(() -> handler.handle(record(orderPlaced(), headers(EventHeaders.FAIL_FATAL, "true"))))
                .doesNotThrowAnyException();
        assertThat(counters.receivedCount()).isEqualTo(1);
    }

    private static OrderPlaced orderPlaced() {
        return OrderPlaced.of("order-" + System.nanoTime(), "customer-1",
                List.of(new OrderLine("SKU-1", 2, new BigDecimal("9.90"))), "EUR");
    }

    private static Headers headers(String name, String value) {
        Headers headers = new RecordHeaders();
        headers.add(name, value.getBytes(StandardCharsets.UTF_8));
        return headers;
    }

    private static ConsumerRecord<String, OrderPlaced> record(OrderPlaced event, Headers headers) {
        if (event != null) {
            headers.add(EventHeaders.EVENT_ID,
                    event.envelope().eventId().toString().getBytes(StandardCharsets.UTF_8));
        }
        return new ConsumerRecord<>(
                TOPIC, 0, 0L, System.currentTimeMillis(), TimestampType.CREATE_TIME, -1, -1,
                event == null ? "order-1" : event.partitionKey(), event, headers, Optional.empty());
    }
}

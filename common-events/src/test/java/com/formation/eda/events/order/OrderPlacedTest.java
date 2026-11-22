package com.formation.eda.events.order;

import com.formation.eda.events.EventEnvelope;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderPlacedTest {

    private static final List<OrderLine> TWO_LINES = List.of(
            new OrderLine("SKU-1", 2, new BigDecimal("19.99")),
            new OrderLine("SKU-2", 1, new BigDecimal("5.00")));

    @Test
    @DisplayName("la fabrique calcule le total et genere une enveloppe racine")
    void factoryComputesTotalAndBuildsRootEnvelope() {
        OrderPlaced event = OrderPlaced.of("order-1", "customer-7", TWO_LINES, "EUR");

        assertThat(event.totalAmount()).isEqualByComparingTo("44.98");
        assertThat(event.envelope().eventType()).isEqualTo("OrderPlaced");
        assertThat(event.envelope().aggregateId()).isEqualTo("order-1");
        assertThat(event.envelope().version()).isEqualTo(1);
        assertThat(event.envelope().isRoot()).isTrue();
    }

    @Test
    @DisplayName("la cle de partition est l'orderId : l'ordre est garanti par commande")
    void partitionKeyIsTheOrderId() {
        OrderPlaced event = OrderPlaced.of("order-42", "customer-7", TWO_LINES, "EUR");

        assertThat(event.partitionKey()).isEqualTo("order-42");
    }

    @Test
    @DisplayName("les lignes sont recopiees : modifier la liste d'origine ne change pas l'evenement")
    void linesAreDefensivelyCopied() {
        List<OrderLine> mutable = new ArrayList<>(TWO_LINES);
        OrderPlaced event = OrderPlaced.of("order-1", "customer-7", mutable, "EUR");

        mutable.clear();

        assertThat(event.lines()).hasSize(2);
        assertThatThrownBy(() -> event.lines().add(new OrderLine("SKU-3", 1, BigDecimal.ONE)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @DisplayName("un evenement invalide ne peut pas exister")
    void invalidEventsAreRejected() {
        EventEnvelope envelope = EventEnvelope.root(OrderPlaced.TYPE, "order-1", 1);

        assertThatThrownBy(() -> new OrderPlaced(envelope, "order-1", "c-1", List.of(), BigDecimal.TEN, "EUR"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sans ligne");

        assertThatThrownBy(() -> new OrderPlaced(envelope, "order-1", "c-1", TWO_LINES, BigDecimal.ZERO, "EUR"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("totalAmount");

        assertThatThrownBy(() -> new OrderLine("SKU-1", 0, BigDecimal.ONE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("quantity");
    }
}

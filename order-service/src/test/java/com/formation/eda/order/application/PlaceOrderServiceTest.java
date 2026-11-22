package com.formation.eda.order.application;

import com.formation.eda.events.DomainEvent;
import com.formation.eda.events.order.OrderLine;
import com.formation.eda.events.order.OrderPlaced;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Le cas d'usage se teste sans broker : c'est tout l'interet du port {@link EventPublisher}.
 * Les tests avec un vrai Kafka arrivent en phase 10.
 */
class PlaceOrderServiceTest {

    /** Publisher d'essai : retient le dernier envoi au lieu de l'emettre. */
    private static final class CapturingPublisher implements EventPublisher {
        private String topic;
        private DomainEvent event;

        @Override
        public Publication publish(String topic, DomainEvent event) {
            this.topic = topic;
            this.event = event;
            return new Publication(topic, 2, 17L);
        }
    }

    private final CapturingPublisher publisher = new CapturingPublisher();
    private final PlaceOrderService service = new PlaceOrderService(publisher);

    @Test
    @DisplayName("publie un OrderPlaced sur shop.order.events.v1, cle = orderId")
    void publishesOrderPlacedKeyedByOrderId() {
        var placed = service.placeOrder(
                "customer-7",
                List.of(new OrderLine("SKU-1", 2, new BigDecimal("10.00"))),
                "EUR");

        assertThat(publisher.topic).isEqualTo("shop.order.events.v1");
        assertThat(publisher.event).isInstanceOf(OrderPlaced.class);
        // La cle est l'orderId, pas le customerId : l'ordre est garanti par commande.
        assertThat(publisher.event.partitionKey()).isEqualTo(placed.orderId());
        assertThat(publisher.event.envelope().eventId()).isEqualTo(placed.eventId());
        assertThat(publisher.event.envelope().aggregateId()).isEqualTo(placed.orderId());

        OrderPlaced event = (OrderPlaced) publisher.event;
        assertThat(event.totalAmount()).isEqualByComparingTo("20.00");
        assertThat(event.customerId()).isEqualTo("customer-7");
    }

    @Test
    @DisplayName("deux commandes du meme client recoivent deux orderId distincts")
    void eachOrderGetsItsOwnIdentity() {
        List<OrderLine> lines = List.of(new OrderLine("SKU-1", 1, BigDecimal.ONE));

        var first = service.placeOrder("customer-7", lines, "EUR");
        var second = service.placeOrder("customer-7", lines, "EUR");

        assertThat(first.orderId()).isNotEqualTo(second.orderId());
        // Consequence : deux commandes du meme client peuvent partir sur deux partitions
        // differentes et etre traitees en parallele. L'ordre n'est garanti que par commande.
    }
}

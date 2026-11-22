package com.formation.eda.order.api;

import com.formation.eda.order.application.PlaceOrderService.PlacedOrder;

import java.util.UUID;

/**
 * Reponse de {@code POST /orders}.
 *
 * <p>Le statut est {@code ACCEPTED}, pas {@code CONFIRMED} : au retour de l'appel, la commande
 * est un fait publie, rien de plus. Le paiement, le stock et l'expedition n'ont pas encore
 * ete decides. C'est la contrepartie directe du decouplage : l'API ne peut pas promettre ce
 * qu'elle ne controle plus.
 *
 * <p>{@code partition} et {@code offset} n'auraient rien a faire dans une vraie API — c'est
 * de la plomberie Kafka exposee a un client HTTP. Ils sont la pour l'experience P1-T4, afin
 * d'observer la repartition des cles sans ouvrir Kafka UI.
 */
public record PlaceOrderResponse(
        String orderId,
        String status,
        UUID eventId,
        String topic,
        int partition,
        long offset) {

    public static PlaceOrderResponse from(PlacedOrder placed) {
        return new PlaceOrderResponse(
                placed.orderId(),
                "ACCEPTED",
                placed.eventId(),
                placed.publication().topic(),
                placed.publication().partition(),
                placed.publication().offset());
    }
}

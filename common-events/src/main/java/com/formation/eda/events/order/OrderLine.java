package com.formation.eda.events.order;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Une ligne de commande, telle qu'elle a ete acceptee au moment du {@code OrderPlaced}.
 *
 * <p>Le prix est <b>recopie dans l'evenement</b>, pas reference. Un evenement raconte un
 * fait passe : si le catalogue change le prix demain, la commande d'aujourd'hui garde le
 * prix d'aujourd'hui. C'est aussi ce qui permet a inventory-service et shipping-service de
 * travailler sans appeler order-service ni le catalogue.
 */
public record OrderLine(String sku, int quantity, BigDecimal unitPrice) {

    public OrderLine {
        if (sku == null || sku.isBlank()) {
            throw new IllegalArgumentException("sku ne peut pas etre vide");
        }
        if (quantity < 1) {
            throw new IllegalArgumentException("quantity doit etre >= 1, recu : " + quantity);
        }
        Objects.requireNonNull(unitPrice, "unitPrice");
        if (unitPrice.signum() < 0) {
            throw new IllegalArgumentException("unitPrice ne peut pas etre negatif : " + unitPrice);
        }
    }

    public BigDecimal lineTotal() {
        return unitPrice.multiply(BigDecimal.valueOf(quantity));
    }
}

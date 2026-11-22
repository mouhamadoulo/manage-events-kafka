package com.formation.eda.events.order;

import com.formation.eda.events.DomainEvent;
import com.formation.eda.events.EventEnvelope;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

/**
 * Premier evenement de la saga : une commande a ete acceptee par order-service.
 *
 * <p>Nom au passe (CLAUDE.md §4) : c'est un fait, pas une demande. Aucun consumer n'est
 * oblige d'y reagir, et order-service ne sait pas qui ecoute. C'est toute la difference
 * avec un appel REST a payment-service.
 *
 * <p>L'evenement est <b>autoportant</b> : il contient tout ce dont payment-service,
 * inventory-service et shipping-service ont besoin. Un consumer qui devrait rappeler
 * order-service pour completer l'information recreerait le couplage temporel qu'on cherche
 * a supprimer.
 */
public record OrderPlaced(
        EventEnvelope envelope,
        String orderId,
        String customerId,
        List<OrderLine> lines,
        BigDecimal totalAmount,
        String currency) implements DomainEvent {

    /** Version du contrat. Passera a 2 en phase 8 (upcasting d'evenements). */
    public static final int VERSION = 1;

    /** Nom logique, utilise par les consumers pour router. Decorrele du nom de classe. */
    public static final String TYPE = "OrderPlaced";

    public OrderPlaced {
        Objects.requireNonNull(envelope, "envelope");
        requireText(orderId, "orderId");
        requireText(customerId, "customerId");
        requireText(currency, "currency");
        Objects.requireNonNull(lines, "lines");
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("une commande sans ligne n'a pas de sens");
        }
        // Copie defensive : un record n'est immuable que si ses composants le sont, et une
        // List passee par l'appelant peut etre modifiee apres construction.
        lines = List.copyOf(lines);
        Objects.requireNonNull(totalAmount, "totalAmount");
        if (totalAmount.signum() <= 0) {
            throw new IllegalArgumentException("totalAmount doit etre > 0, recu : " + totalAmount);
        }
    }

    /**
     * Fabrique un evenement racine : total calcule, enveloppe generee.
     *
     * <p>Le total est calcule ici et stocke dans l'evenement plutot que recalcule par chaque
     * consumer : si la regle de calcul change (remise, taxe), les evenements deja publies
     * doivent garder le montant sur lequel le client s'est engage.
     */
    public static OrderPlaced of(String orderId, String customerId, List<OrderLine> lines, String currency) {
        Objects.requireNonNull(lines, "lines");
        BigDecimal total = lines.stream()
                .map(OrderLine::lineTotal)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        return new OrderPlaced(
                EventEnvelope.root(TYPE, orderId, VERSION),
                orderId,
                customerId,
                lines,
                total,
                currency);
    }

    /**
     * Cle de partition = orderId.
     *
     * <p>WHY: tous les evenements d'une commande doivent arriver dans l'ordre ou ils se sont
     * produits ({@code OrderPlaced} avant {@code OrderCancelled}). Meme cle = meme partition
     * = ordre garanti. Cle nulle = repartition round-robin = desordre possible sur une meme
     * commande, donc interdite sur un topic metier (CLAUDE.md §2, regle d'or).
     */
    @Override
    public String partitionKey() {
        return orderId;
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " ne peut pas etre vide");
        }
    }
}

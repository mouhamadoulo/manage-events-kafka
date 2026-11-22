package com.formation.eda.order.api;

import com.formation.eda.events.order.OrderLine;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.List;

/**
 * Corps de {@code POST /orders}.
 *
 * <p>WHY un DTO distinct de l'evenement : le contrat de l'API HTTP et le contrat de
 * l'evenement Kafka n'ont ni les memes clients ni le meme rythme de changement. Si le
 * controleur acceptait directement un {@code OrderPlaced}, le moindre ajout de champ dans
 * l'API partirait dans le topic, et toute evolution du topic casserait l'API.
 */
public record PlaceOrderRequest(
        @NotBlank String customerId,
        @NotEmpty @Valid List<Line> lines,
        String currency) {

    public record Line(
            @NotBlank String sku,
            @Min(1) int quantity,
            @NotNull @DecimalMin("0.0") BigDecimal unitPrice) {
    }

    public List<OrderLine> toOrderLines() {
        return lines.stream()
                .map(line -> new OrderLine(line.sku(), line.quantity(), line.unitPrice()))
                .toList();
    }

    public String currencyOrDefault() {
        return currency == null || currency.isBlank() ? "EUR" : currency;
    }
}

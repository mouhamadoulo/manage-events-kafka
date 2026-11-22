package com.formation.eda.order.api;

import com.formation.eda.events.order.OrderLine;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.util.List;

/**
 * Corps de {@code POST /orders}.
 *
 * <p>WHY un DTO distinct de l'evenement : le contrat de l'API HTTP et le contrat de
 * l'evenement Kafka n'ont ni les memes clients ni le meme rythme de changement. Si le
 * controleur acceptait directement un {@code OrderPlaced}, le moindre ajout de champ dans
 * l'API partirait dans le topic, et toute evolution du topic casserait l'API.
 *
 * <h2>Limites assumees de ce laboratoire</h2>
 * <p>Deux choses seraient inacceptables dans un vrai systeme et sont ici volontaires :
 * <ul>
 *   <li><b>le prix est fourni par le client.</b> Un vrai order-service lit le prix dans un
 *       catalogue cote serveur a partir du seul {@code sku} : accepter un prix du client,
 *       c'est accepter qu'il paie ce qu'il decide. Ici il n'y a ni catalogue ni paiement
 *       reel, et l'objectif de la phase 1 est le partitionnement, pas la tarification ;</li>
 *   <li><b>le {@code customerId} est fourni par le client</b> et l'endpoint n'est pas
 *       authentifie : n'importe qui pourrait commander au nom d'un autre. L'authentification
 *       arrive en phase 12 ; le {@code customerId} viendra alors du jeton, pas du corps.</li>
 * </ul>
 * <p>Ces deux points sont la raison pour laquelle le service n'ecoute que sur
 * {@code 127.0.0.1} (cf. CLAUDE.md §1) et ne doit pas etre expose.
 *
 * <p>Les bornes ci-dessous, en revanche, ne sont pas negociables meme en labo : sans
 * {@code @Digits}, un {@code BigDecimal} a 10 000 chiffres de decimale est accepte, serialise
 * dans le topic et deserialise par chaque consumer — un deni de service a un seul POST.
 */
public record PlaceOrderRequest(
        @NotBlank @Size(max = 64) String customerId,
        @NotEmpty @Size(max = 100) @Valid List<Line> lines,
        @Size(min = 3, max = 3) String currency) {

    public record Line(
            @NotBlank @Size(max = 64) String sku,
            @Min(1) @Max(1_000) int quantity,
            @NotNull @Positive @DecimalMax("1000000.00") @Digits(integer = 7, fraction = 2)
            BigDecimal unitPrice) {
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

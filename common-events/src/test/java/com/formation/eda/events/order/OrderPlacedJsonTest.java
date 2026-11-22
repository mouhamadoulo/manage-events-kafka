package com.formation.eda.events.order;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Un evenement est un contrat reseau avant d'etre une classe Java : s'il ne fait pas
 * l'aller-retour JSON, il ne traversera pas Kafka. Test de contrat, des la phase 1.
 */
class OrderPlacedJsonTest {

    // Jackson 3 : ObjectMapper immuable, construit par builder (cf. CLAUDE.md §5).
    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    @DisplayName("aller-retour JSON sans perte")
    void roundTripsWithoutLoss() {
        OrderPlaced original = OrderPlaced.of(
                "order-1",
                "customer-7",
                List.of(new OrderLine("SKU-1", 2, new BigDecimal("19.99"))),
                "EUR");

        String json = mapper.writeValueAsString(original);
        OrderPlaced reread = mapper.readValue(json, OrderPlaced.class);

        assertThat(reread).isEqualTo(original);
        // Instant conserve a la nanoseconde : Jackson 3 ecrit de l'ISO-8601, pas un timestamp.
        assertThat(reread.envelope().occurredAt()).isEqualTo(original.envelope().occurredAt());
        assertThat(json).contains("\"occurredAt\":\"20");
        assertThat(reread.totalAmount()).isEqualByComparingTo(original.totalAmount());
    }

    @Test
    @DisplayName("le JSON ne contient que les champs declares : rien ne fuit par accident")
    void jsonCarriesExactlyTheDeclaredFields() {
        OrderPlaced event = OrderPlaced.of(
                "order-1", "customer-7", List.of(new OrderLine("SKU-1", 1, BigDecimal.ONE)), "EUR");

        @SuppressWarnings("unchecked")
        Map<String, Object> asMap = mapper.readValue(mapper.writeValueAsString(event), Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> envelope = (Map<String, Object>) asMap.get("envelope");

        assertThat(asMap).containsOnlyKeys(
                "envelope", "orderId", "customerId", "lines", "totalAmount", "currency");
        // "root" apparaitrait ici si isRoot() n'etait pas @JsonIgnore : un champ non declare
        // dans le contrat, qui finirait publie dans le Schema Registry en phase 2.
        assertThat(envelope).containsOnlyKeys(
                "eventId", "eventType", "occurredAt", "correlationId",
                "causationId", "aggregateId", "version");
    }

    @Test
    @DisplayName("la validation du record s'applique aussi a la deserialisation")
    void validationAlsoAppliesWhenReadingFromTheWire() {
        // Un producteur buggue (ou un autre langage) peut envoyer une commande sans ligne.
        String invalid = """
                {"envelope":{"eventId":"0b1b8dc7-4e1f-4a65-9e84-1f9a0a0e6f10","eventType":"OrderPlaced",\
                "occurredAt":"2026-11-22T10:00:00Z","correlationId":"0b1b8dc7-4e1f-4a65-9e84-1f9a0a0e6f10",\
                "causationId":null,"aggregateId":"order-1","version":1},\
                "orderId":"order-1","customerId":"c-1","lines":[],"totalAmount":10,"currency":"EUR"}""";

        assertThat(catchThrowableOf(invalid)).isNotNull();
    }

    private Throwable catchThrowableOf(String json) {
        try {
            mapper.readValue(json, OrderPlaced.class);
            return null;
        } catch (RuntimeException e) {
            // Jackson enveloppe l'IllegalArgumentException du constructeur compact.
            System.out.println("Refus a la deserialisation : " + e.getClass().getSimpleName());
            return e;
        }
    }
}

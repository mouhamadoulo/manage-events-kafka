package com.formation.eda.events;

/**
 * Noms de topics, en un seul endroit.
 *
 * <p>WHY: {@code auto.create.topics.enable=false} sur le broker (cf. docker-compose).
 * Une faute de frappe dans un nom de topic ne cree donc pas un topic fantome a une
 * partition : elle fait echouer la publication. Des constantes partagees evitent que
 * producer et consumer divergent d'un caractere.
 *
 * <p>Convention : {@code <domaine>.<agregat>.<type>.v<version>} (CLAUDE.md §2). Le suffixe
 * de version fait partie du nom : une evolution incompatible donnera un {@code .v2}
 * publie en parallele, pas une rupture en place.
 */
public final class Topics {

    public static final String ORDER_EVENTS = "shop.order.events.v1";
    public static final String ORDER_STORE = "shop.order.store.v1";
    public static final String PAYMENT_EVENTS = "shop.payment.events.v1";
    public static final String INVENTORY_EVENTS = "shop.inventory.events.v1";
    public static final String SHIPPING_EVENTS = "shop.shipping.events.v1";

    /** Topic de rejeu etage (phase 4). {@code n} commence a 1. */
    public static String retry(String topic, int n) {
        return topic + ".retry-" + n;
    }

    /** Dead letter topic (phase 4). */
    public static String deadLetter(String topic) {
        return topic + ".dlt";
    }

    private Topics() {
    }
}

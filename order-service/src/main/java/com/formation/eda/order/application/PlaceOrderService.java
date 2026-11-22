package com.formation.eda.order.application;

import com.formation.eda.events.Topics;
import com.formation.eda.events.order.OrderLine;
import com.formation.eda.events.order.OrderPlaced;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

/**
 * Cas d'usage « placer une commande ».
 *
 * <p>Phase 1 : la commande n'est pas persistee. order-service se contente de la transformer
 * en fait publie. L'agregat, l'event store et la projection arrivent en phase 8 ; l'outbox en
 * phase 5. Ce qui est deja vrai ici et ne changera plus : order-service ne sait pas qui
 * consomme {@code OrderPlaced}, et n'appelle aucun autre service.
 */
@Service
public class PlaceOrderService {

    /** Resultat renvoye a l'API : l'identite creee et l'endroit ou le fait a ete ecrit. */
    public record PlacedOrder(String orderId, UUID eventId, EventPublisher.Publication publication) {
    }

    private final EventPublisher eventPublisher;

    public PlaceOrderService(EventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    public PlacedOrder placeOrder(String customerId, List<OrderLine> lines, String currency) {
        // L'identite est generee cote serveur : elle devient la cle de partition, donc elle
        // doit exister AVANT la publication. Un identifiant attribue plus tard par une base
        // obligerait a publier sans cle (ordre perdu) ou a attendre l'insert.
        String orderId = "order-" + UUID.randomUUID();

        OrderPlaced event = OrderPlaced.of(orderId, customerId, lines, currency);
        EventPublisher.Publication publication = eventPublisher.publish(Topics.ORDER_EVENTS, event);

        return new PlacedOrder(orderId, event.envelope().eventId(), publication);
    }
}

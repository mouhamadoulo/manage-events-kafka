package com.formation.eda.order.api;

import com.formation.eda.order.application.EventPublicationFailedException;
import com.formation.eda.order.application.PlaceOrderService;
import com.formation.eda.order.application.PlaceOrderService.PlacedOrder;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final PlaceOrderService placeOrderService;

    public OrderController(PlaceOrderService placeOrderService) {
        this.placeOrderService = placeOrderService;
    }

    /**
     * 202 Accepted, et non 201 Created.
     *
     * <p>Rien n'a ete cree que le client puisse aller lire : la commande vient d'etre
     * <b>acceptee</b> et publiee. Elle sera traitee par d'autres services, plus tard. Repondre
     * 201 laisserait croire a une ressource immediatement coherente, ce que la coherence a
     * terme ne permet pas de promettre.
     */
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PlaceOrderResponse placeOrder(@Valid @RequestBody PlaceOrderRequest request) {
        PlacedOrder placed = placeOrderService.placeOrder(
                request.customerId(),
                request.toOrderLines(),
                request.currencyOrDefault());
        return PlaceOrderResponse.from(placed);
    }

    /**
     * 503 et non 500 : la commande n'a pas ete prise, mais rien n'est casse cote serveur.
     * Le client peut reessayer, et comme l'{@code eventId} est genere avant l'envoi, un
     * reessai cote serveur serait meme detectable.
     *
     * <p>Attention a ce que ce handler ne dit PAS : l'absence d'erreur ne prouve pas que
     * l'evenement est ecrit. Avec {@code acks=0}, le producteur ne le sait pas lui-meme
     * (cf. experience P3-T3).
     */
    @ExceptionHandler(EventPublicationFailedException.class)
    public ProblemDetail onPublicationFailure(EventPublicationFailedException exception) {
        ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.SERVICE_UNAVAILABLE);
        problem.setTitle("Commande non prise");
        // WHY un message generique : le message d'origine vient du client Kafka et nomme
        // l'infrastructure (hote et port du broker, du Schema Registry, sujet, timeouts).
        // Le client HTTP n'en a pas besoin pour reessayer, et cela lui apprendrait la
        // topologie interne. Le detail reste cote serveur, deja journalise avec l'eventId
        // par KafkaEventPublisher.
        problem.setDetail("Publication indisponible, reessayez.");
        return problem;
    }

    /**
     * Les invariants des records d'evenements levent {@code IllegalArgumentException}. Sans ce
     * handler, une commande refusee par le contrat d'evenement sortirait en 500 — une erreur
     * serveur pour une faute du client.
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail onInvalidOrder(IllegalArgumentException exception) {
        ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        problem.setTitle("Commande invalide");
        problem.setDetail(exception.getMessage());
        return problem;
    }
}

package com.formation.eda.notification.api;

import com.formation.eda.events.Topics;
import com.formation.eda.notification.application.DltReplayService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Console d'administration du dead letter topic : regarder, puis rejouer.
 *
 * <p><b>Cette console n'est pas authentifiee</b>, comme le reste du laboratoire : Kafka UI
 * tourne avec {@code DYNAMIC_CONFIG_ENABLED} et Postgres est en {@code eda/eda}. Ce qui la rend
 * acceptable ici est que le service est lie au <b>loopback</b> ({@code server.address} dans
 * l'{@code application.yml}) — et rien d'autre. Elle peut republier des messages dans un topic
 * metier : exposee sur le reseau, c'est un moyen d'injection. La mettre derriere un jeton est
 * <b>P12-T8</b>.
 *
 * <p>Ce bind a ete ajoute apres coup : la premiere version de ce javadoc affirmait que le
 * service n'ecoutait que sur {@code 127.0.0.1}, alors que Spring Boot ecoute par defaut sur
 * {@code 0.0.0.0}. La regle de CLAUDE.md §1 etait tenue pour les ports publies par Docker, pas
 * pour les services lances sur l'hote.
 *
 * <p>Deux garde-fous qui restent valables quoi qu'il arrive :
 * <ul>
 *   <li>borne dure a {@value com.formation.eda.notification.application.DltReplayService#MAX_OFFSETS_PER_CALL}
 *       offsets par appel — un appel ne peut pas faire tourner la memoire du service ;
 *   <li>un message sans {@code x-origin-topic} est refuse, donc rien ne peut etre republie sur
 *       un topic choisi par l'appelant : la destination vient du message, pas de la requete.
 * </ul>
 *
 * <p>Le nom du topic est contraint par expression reguliere : seul un topic du domaine
 * {@code shop.} finissant par {@code .dlt} est lisible. Sans cela, l'endpoint serait un
 * lecteur universel de topics, y compris {@code _schemas} ou les topics internes du broker.
 */
@RestController
@RequestMapping("/admin/dlt")
public class DltAdminController {

    private final DltReplayService replayService;

    public DltAdminController(DltReplayService replayService) {
        this.replayService = replayService;
    }

    /** Corps de la demande de rejeu. La destination n'y figure pas : elle vient du message. */
    public record ReplayBody(
            @Pattern(regexp = "^shop\\.[a-z0-9.\\-]+\\.dlt$",
                    message = "seul un dead letter topic du domaine shop. est lisible")
            String topic,

            @Min(0) @Max(1000)
            int partition,

            @NotEmpty
            @Size(max = DltReplayService.MAX_OFFSETS_PER_CALL,
                    message = "au plus " + DltReplayService.MAX_OFFSETS_PER_CALL + " offsets par appel")
            List<@jakarta.validation.constraints.NotNull @Min(0) Long> offsets) {
    }

    /**
     * Une demande mal formee est une erreur du CLIENT, pas du service.
     *
     * <p>Sans cela, {@code offsets:[null]} — ce qu'envoie un client qui serialise une variable
     * vide — remontait en 500. Un 500 dit « mon service a un bug » ; ici le bug est dans la
     * requete, et le message doit le dire.
     */
    @org.springframework.web.bind.annotation.ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<java.util.Map<String, String>> onInvalidRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(java.util.Map.of("erreur", e.getMessage()));
    }

    /**
     * Les derniers messages d'un dead letter topic, avec leur cause.
     *
     * @param topic defaut : la DLT des evenements de commande
     * @param limit borne a {@value com.formation.eda.notification.application.DltReplayService#MAX_OFFSETS_PER_CALL}
     */
    @GetMapping
    public List<DltReplayService.DltRecordView> peek(
            @RequestParam(defaultValue = Topics.ORDER_EVENTS_DLT)
            @Pattern(regexp = "^shop\\.[a-z0-9.\\-]+\\.dlt$") String topic,
            @RequestParam(defaultValue = "20") @Min(1) @Max(DltReplayService.MAX_OFFSETS_PER_CALL) int limit) {
        return replayService.peek(topic, limit);
    }

    /**
     * Republie les offsets demandes sur le topic d'origine inscrit dans chaque message.
     *
     * <p>Le rejeu n'est pas idempotent par lui-meme : il republie a chaque appel. C'est le
     * <b>consumer</b> qui deduplique, par {@code processed_event}. Donc rejouer deux fois ne
     * produit qu'un effet metier, et le rapport annonce bien 1 republication a chaque fois.
     */
    @PostMapping("/replay")
    public ResponseEntity<DltReplayService.ReplayReport> replay(@Valid @RequestBody ReplayBody body) {
        DltReplayService.ReplayReport report = replayService.replay(
                new DltReplayService.ReplayRequest(body.topic(), body.partition(), body.offsets()));
        return ResponseEntity.ok(report);
    }
}

package com.formation.eda.kafka.reliability;

/**
 * Echec <b>definitif</b> du traitement d'un evenement : le reessayer ne changera rien. Message
 * hors contrat, reference inconnue, incoherence que seul un correctif de code ou une
 * intervention humaine resoudra.
 *
 * <p>Un evenement qui leve cette exception part directement en dead letter topic, sans passer
 * par les etages de retry.
 *
 * <p>WHY cette distinction : reessayer un echec definitif coute deux fois. Le travail inutile
 * d'abord, mais surtout le retard — pendant les 35 secondes de retry etage, les evenements
 * suivants de la meme partition attendent, et le probleme reel n'est signale qu'a la fin.
 */
public class NonRetryableEventException extends RuntimeException {

    public NonRetryableEventException(String message) {
        super(message);
    }

    public NonRetryableEventException(String message, Throwable cause) {
        super(message, cause);
    }
}

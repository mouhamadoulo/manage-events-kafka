package com.formation.eda.kafka.reliability;

/**
 * Echec <b>transitoire</b> du traitement d'un evenement : la meme operation a une chance
 * raisonnable de reussir plus tard. Base injoignable, service aval en cours de redemarrage,
 * verrou momentane.
 *
 * <p>WHY une exception dediee plutot que de s'appuyer sur le type de l'exception technique :
 * le classement retryable / non-retryable est une decision METIER que le code de traitement
 * est seul a pouvoir prendre. Une {@code SQLException} peut etre transitoire (connexion
 * perdue) ou definitive (contrainte violee par une donnee qui ne changera pas). Laisser le
 * classement aux types techniques revient a reessayer indefiniment des echecs definitifs.
 *
 * <p>Ce n'est <b>pas</b> le vehicule d'un refus metier. Un paiement refuse n'est pas une
 * panne : c'est la branche de compensation de la saga (phase 6), publiee comme un evenement,
 * pas levee comme une exception.
 */
public class TransientProcessingException extends RuntimeException {

    public TransientProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}

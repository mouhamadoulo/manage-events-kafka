package com.formation.eda.order.application;

/**
 * La publication a echoue et le producteur l'a dit.
 *
 * <p>WHY une exception dediee : un echec de publication n'est pas un bug du serveur, c'est un
 * refus de prendre la commande. L'API doit repondre 503 et le client peut reessayer — le
 * {@code eventId} etant genere avant l'envoi, un reessai a l'identique serait meme detectable.
 * Laisser remonter une {@code CompletionException} donnerait un 500 et une trace illisible.
 *
 * <p>Ce qu'elle ne couvre pas : les cas ou le producteur <b>ne sait pas</b>. Avec
 * {@code acks=0}, l'absence d'exception ne prouve rien (cf. experience P3-T3).
 */
public class EventPublicationFailedException extends RuntimeException {

    public EventPublicationFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}

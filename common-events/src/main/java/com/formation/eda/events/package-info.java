/**
 * Contrats d'evenements partages entre tous les services.
 *
 * <p>Regles (cf. CLAUDE.md §4) :
 * <ul>
 *   <li>un evenement est un {@code record} immuable, jamais une entite JPA ;</li>
 *   <li>nom au passe ({@code OrderPlaced}), car un evenement raconte un fait accompli ;</li>
 *   <li>enveloppe obligatoire : eventId, eventType, occurredAt, correlationId,
 *       causationId, aggregateId, version ;</li>
 *   <li>tout changement de forme passe par le Schema Registry en compatibilite BACKWARD.</li>
 * </ul>
 *
 * <p>Rempli en phase 1 (P1-T1) puis phase 2 (schemas JSON).
 */
package com.formation.eda.events;

package com.formation.eda.kafka.reliability;

import java.util.UUID;

/**
 * Registre des evenements deja traites par un groupe de consumers.
 *
 * <p>Kafka livre <b>au moins une fois</b>. Un rebalancing, un redemarrage, un reset d'offsets
 * (272 evenements retraites en P1-T6) ou un rejeu depuis la DLT font repasser les memes
 * evenements. Sans registre, chaque passage produit un nouvel effet metier : un second mail,
 * un second debit.
 */
public interface ProcessedEventStore {

    /**
     * Enregistre que {@code consumerGroup} prend en charge {@code eventId}.
     *
     * <p>L'operation est <b>atomique</b> : elle marque et repond en une seule ecriture.
     *
     * <p>WHY pas de {@code SELECT} prealable : {@code SELECT} puis {@code INSERT} est une
     * condition de course. Deux instances du meme groupe peuvent passer le test en meme temps
     * et traiter l'evenement deux fois — ce qui est exactement le bug que ce registre existe
     * pour empecher. La contrainte d'unicite de la base est le seul arbitre.
     *
     * @param eventId       identite de la publication, lue dans le header {@code eventId}
     * @param consumerGroup groupe qui traite. Fait partie de la cle : en phase 6, les 5
     *                      services de la saga lisent les memes topics dans 5 groupes, et
     *                      chacun doit traiter l'evenement une fois.
     * @param sourceTopic   topic d'ou vient le record. Journalise, mais <b>hors de la cle</b> :
     *                      un record venu de {@code .retry-1} est le meme evenement, sinon
     *                      chaque etage de retry rejouerait l'effet metier.
     * @return {@code true} si la ligne a ete inseree, donc s'il faut traiter.
     *         {@code false} si elle existait deja, donc doublon a sauter.
     */
    boolean markProcessed(UUID eventId, String consumerGroup, String sourceTopic);

    /**
     * Retire la marque, pour qu'un retry puisse retraiter l'evenement.
     *
     * <p>WHY cette operation existe : la marque est posee <b>avant</b> le traitement, sinon un
     * crash entre le traitement et le marquage rejouerait l'effet. Mais marquer avant signifie
     * qu'un echec laisserait une marque sur un travail jamais fait : le retry serait classe
     * doublon et le message perdu <b>en silence</b>. L'echec doit donc defaire la marque.
     *
     * <p>Limite assumee : un crash brutal entre le marquage et l'echec laisse une marque
     * orpheline, donc un message saute. La vraie reponse est d'ecrire la marque dans la meme
     * transaction que l'effet metier — c'est la phase 5, et c'est sa raison d'etre.
     */
    void forget(UUID eventId, String consumerGroup);
}

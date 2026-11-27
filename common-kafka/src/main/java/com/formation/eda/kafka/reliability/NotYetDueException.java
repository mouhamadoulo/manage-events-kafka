package com.formation.eda.kafka.reliability;

import org.apache.kafka.common.TopicPartition;

/**
 * Signale qu'un record d'un etage de retry n'a pas encore atteint son echeance.
 *
 * <p><b>WHY une exception, alors que le listener pourrait simplement ne rien faire.</b> C'est la
 * decision la plus contre-intuitive de la phase 4, et elle vient d'un bug reel de ce projet.
 *
 * <p>La premiere version du differe faisait {@code consumer.seek()} + {@code consumer.pause()}
 * puis un retour normal. Mais {@link IdempotentRecordInterceptor} a deja marque l'evenement
 * comme pris en charge avant d'appeler le listener, et il ne defait cette marque que dans
 * {@code failure()} — appele uniquement sur exception. Un retour normal laissait donc la marque
 * en place sur un travail jamais fait : au reveil de la partition, l'evenement etait classe
 * doublon et saute. <b>Tous les messages en attente etaient perdus, sans une ligne de log.</b>
 *
 * <p>En levant, trois choses tombent juste d'un coup :
 * <ul>
 *   <li>{@code RecordInterceptor.failure()} part, donc la marque d'idempotence est retiree ;
 *   <li>l'offset n'est <b>pas</b> commite, donc le record sera bien redelivre ;
 *   <li>le gestionnaire d'erreur du conteneur prend la main, et c'est lui qui sait appliquer
 *       un delai sans bloquer de thread — ici {@link PartitionPausingBackOffHandler}, et non
 *       le {@code ContainerPausingBackOffHandler} du framework : voir plus bas.
 * </ul>
 *
 * <p>Elle doit etre classee <b>retryable</b> : non-retryable, elle enverrait en DLT au premier
 * passage un message dont le seul tort est d'etre arrive trop tot.
 */
public class NotYetDueException extends RuntimeException {

    private final long remainingMillis;
    private final TopicPartition topicPartition;

    public NotYetDueException(long remainingMillis) {
        this(remainingMillis, null);
    }

    /**
     * @param topicPartition partition du record en attente, ou {@code null} si elle n'est pas
     *        connue.
     *
     * <p>WHY la partition est portee par l'exception : un {@code BackOffHandler} ne recoit que
     * le conteneur, l'exception et l'intervalle — <b>pas le record</b>. L'exception est donc le
     * seul vehicule possible pour que la pause porte sur la bonne partition plutot que sur tout
     * le conteneur. C'est aussi ce que fait {@code ContainerPausingBackOffHandler} de
     * spring-kafka, et c'est pourquoi il ne peut pas faire mieux : il appelle
     * {@code pause(container)}, qui suspend <b>toutes</b> les partitions assignees, y compris
     * celles dont les records sont deja echus.
     */
    public NotYetDueException(long remainingMillis, TopicPartition topicPartition) {
        super("echeance non atteinte, reste " + remainingMillis + " ms");
        if (remainingMillis < 0) {
            throw new IllegalArgumentException(
                    "remainingMillis doit etre >= 0, recu : " + remainingMillis);
        }
        this.remainingMillis = remainingMillis;
        this.topicPartition = topicPartition;
    }

    /** Duree restante avant echeance. C'est elle qui devient l'intervalle de pause. */
    public long remainingMillis() {
        return remainingMillis;
    }

    /** Partition a mettre en pause, ou {@code null} si inconnue. */
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    /**
     * La trace d'appel n'apporte rien : cette exception est un signal de controle, pas un
     * incident, et elle est levee une fois par record en attente. La remplir coute et pollue
     * les logs.
     */
    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}

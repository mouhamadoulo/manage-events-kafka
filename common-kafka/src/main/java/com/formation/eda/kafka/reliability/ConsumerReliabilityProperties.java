package com.formation.eda.kafka.reliability;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.List;

/**
 * Reglages de la mecanique de fiabilite consumer, prefixe {@code eda.reliability}.
 *
 * <p>WHY des proprietes et non des constantes : les delais d'etage sont la seule chose qu'une
 * experience a besoin de changer. Les mesures de P4-T4 tournent avec 5 s / 30 s, mais un test
 * d'integration ne peut pas attendre 35 secondes — il les ramene a quelques centaines de
 * millisecondes par {@code @DynamicPropertySource}. Un reglage qu'on ne peut pas changer est
 * un reglage qu'on ne peut pas mesurer.
 *
 * @param stageDelays              delai de chaque etage de retry, dans l'ordre. Un element par
 *                                 topic {@code .retry-n} : le premier vaut pour
 *                                 {@code .retry-1}. Le delai se compte depuis la publication
 *                                 d'origine, pas depuis l'arrivee dans l'etage.
 * @param blockingRetries          nombre de reessais <b>bloquants</b> sur le topic principal,
 *                                 avant de passer au premier etage. Volontairement petit : ce
 *                                 retry-la occupe le thread du consumer et retarde les
 *                                 evenements suivants de la partition.
 * @param blockingInitialInterval  premier intervalle du backoff bloquant.
 * @param blockingMultiplier       facteur entre deux intervalles bloquants.
 * @param blockingJitter           dispersion aleatoire ajoutee a chaque intervalle. Sans elle,
 *                                 N instances du meme groupe reessaient a la meme milliseconde
 *                                 et frappent ensemble la dependance qui vient de tomber.
 */
@ConfigurationProperties("eda.reliability")
public record ConsumerReliabilityProperties(

        @DefaultValue({"5s", "30s"})
        List<Duration> stageDelays,

        @DefaultValue("2")
        int blockingRetries,

        @DefaultValue("200ms")
        Duration blockingInitialInterval,

        @DefaultValue("2.0")
        double blockingMultiplier,

        @DefaultValue("50ms")
        Duration blockingJitter) {

    public ConsumerReliabilityProperties {
        if (stageDelays == null || stageDelays.isEmpty()) {
            throw new IllegalArgumentException("au moins un delai d'etage est requis");
        }
        if (blockingRetries < 0) {
            throw new IllegalArgumentException("blockingRetries doit etre >= 0, recu : " + blockingRetries);
        }
        stageDelays = List.copyOf(stageDelays);
    }

    /** Nombre d'etages de retry declares, donc de topics {@code .retry-n} attendus. */
    public int stageCount() {
        return stageDelays.size();
    }

    /**
     * Verifie que le nombre de delais configures correspond au nombre d'etages reellement
     * ecoutes, et echoue au DEMARRAGE sinon.
     *
     * <p>WHY cette verification existe. {@code stage-delays} est documente comme reglable, et
     * c'est volontaire — un reglage qu'on ne peut pas changer est un reglage qu'on ne peut pas
     * mesurer. Mais les topics et les listeners, eux, sont figes. Avec trois delais pour deux
     * etages ecoutes, un echec dans {@code .retry-2} est route vers {@code .retry-3} : topic
     * inexistant ({@code auto.create.topics.enable=false}), publication en echec, et comme le
     * recoverer est configure pour remonter l'erreur, le record est rejoue en boucle sur
     * {@code .retry-2}. C'est une <b>partition bloquee</b>, c'est-a-dire precisement ce que
     * cette phase supprime. Symetriquement, un seul delai envoie en DLT tout ce qui atterrit
     * dans {@code .retry-2}, sans un mot.
     *
     * <p>Passer {@code stageCount} en parametre a {@link RetryTopology} accordait la topologie
     * aux <i>delais</i> ; il manquait de l'accorder aux <i>listeners</i>.
     *
     * @param listenedStages nombre d'etages pour lesquels un listener existe reellement
     * @throws IllegalStateException en cas de desaccord
     */
    public void requireStageCount(int listenedStages) {
        if (stageCount() != listenedStages) {
            throw new IllegalStateException(
                    "eda.reliability.stage-delays declare " + stageCount()
                            + " etage(s) de retry, mais " + listenedStages
                            + " sont ecoutes. Un etage sans listener bloque sa partition ; "
                            + "un listener sans delai envoie en DLT sans attendre.");
        }
    }

    /**
     * @param stage numero d'etage, a partir de 1 (cf. convention de nommage {@code .retry-1})
     * @throws IllegalArgumentException si aucun delai n'est declare pour cet etage — mieux vaut
     *         echouer au demarrage que mettre un record en attente d'une echeance inventee
     */
    public Duration delayOfStage(int stage) {
        if (stage < 1 || stage > stageDelays.size()) {
            throw new IllegalArgumentException(
                    "etage de retry inconnu : " + stage + " (etages declares : 1.." + stageDelays.size() + ")");
        }
        return stageDelays.get(stage - 1);
    }
}

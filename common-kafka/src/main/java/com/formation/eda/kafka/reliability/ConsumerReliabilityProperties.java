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

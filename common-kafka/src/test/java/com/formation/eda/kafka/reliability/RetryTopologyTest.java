package com.formation.eda.kafka.reliability;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Les noms de topics de retry sont figes par l'infra : {@code create-topics.ps1} les a crees et
 * {@code register-schemas.ps1} a enregistre un sujet par topic. Une faute d'un caractere ici ne
 * cree pas un topic fantome — {@code auto.create.topics.enable=false} — elle fait echouer la
 * publication du message en echec, donc perd le message qu'on essayait de sauver.
 */
class RetryTopologyTest {

    private static final String MAIN = "shop.order.events.v1";
    private static final int TWO_STAGES = 2;

    @Test
    @DisplayName("le topic principal mene au premier etage")
    void mainTopicLeadsToFirstStage() {
        assertThat(RetryTopology.next(MAIN, TWO_STAGES)).isEqualTo("shop.order.events.v1.retry-1");
    }

    @Test
    @DisplayName("le premier etage mene au second")
    void firstStageLeadsToSecond() {
        assertThat(RetryTopology.next(MAIN + ".retry-1", TWO_STAGES))
                .isEqualTo("shop.order.events.v1.retry-2");
    }

    @Test
    @DisplayName("le dernier etage mene a la dlt")
    void lastStageLeadsToDeadLetterTopic() {
        assertThat(RetryTopology.next(MAIN + ".retry-2", TWO_STAGES))
                .isEqualTo("shop.order.events.v1.dlt");
    }

    @Test
    @DisplayName("avec un seul etage declare, le premier etage mene deja a la dlt")
    void withOneStageTheFirstStageLeadsToTheDeadLetterTopic() {
        // WHY ce test : le nombre d'etages vient de la configuration (eda.reliability
        // .stage-delays). Si la topologie le decidait de son cote, une configuration a un seul
        // delai produirait un .retry-2 dont personne ne consomme — messages perdus en silence.
        assertThat(RetryTopology.next(MAIN + ".retry-1", 1)).isEqualTo("shop.order.events.v1.dlt");
    }

    @Test
    @DisplayName("la dlt n'a pas de suivant")
    void deadLetterTopicHasNoNext() {
        assertThatThrownBy(() -> RetryTopology.next(MAIN + ".dlt", TWO_STAGES))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dlt");
    }

    @Test
    @DisplayName("stageOf lit l'etage depuis le nom du topic")
    void stageOfReadsTheStageFromTheTopicName() {
        assertThat(RetryTopology.stageOf(MAIN)).isZero();
        assertThat(RetryTopology.stageOf(MAIN + ".retry-1")).isEqualTo(1);
        assertThat(RetryTopology.stageOf(MAIN + ".retry-2")).isEqualTo(2);
        assertThat(RetryTopology.stageOf(MAIN + ".dlt")).isEqualTo(-1);
    }

    @Test
    @DisplayName("baseTopicOf retire le suffixe, quelle que soit la forme")
    void baseTopicOfStripsTheSuffix() {
        assertThat(RetryTopology.baseTopicOf(MAIN)).isEqualTo(MAIN);
        assertThat(RetryTopology.baseTopicOf(MAIN + ".retry-1")).isEqualTo(MAIN);
        assertThat(RetryTopology.baseTopicOf(MAIN + ".retry-2")).isEqualTo(MAIN);
        assertThat(RetryTopology.baseTopicOf(MAIN + ".dlt")).isEqualTo(MAIN);
    }

    @Test
    @DisplayName("un suffixe de retry non numerique n'est pas un etage")
    void aNonNumericRetrySuffixIsNotAStage() {
        // Un topic qui contient ".retry-" sans numero lisible n'est pas un etage connu : le
        // traiter comme l'etage 0 vaut mieux que de calculer une echeance sur un numero
        // invente.
        assertThat(RetryTopology.stageOf(MAIN + ".retry-bis")).isZero();
        assertThat(RetryTopology.baseTopicOf(MAIN + ".retry-bis")).isEqualTo(MAIN + ".retry-bis");
    }

    @Test
    @DisplayName("deadLetterOf donne la dlt du topic metier")
    void deadLetterOfGivesTheBusinessTopicDeadLetter() {
        assertThat(RetryTopology.deadLetterOf(MAIN)).isEqualTo("shop.order.events.v1.dlt");
        // Depuis un etage de retry, la dlt reste celle du topic metier, pas « .retry-1.dlt ».
        assertThat(RetryTopology.deadLetterOf(MAIN + ".retry-2")).isEqualTo("shop.order.events.v1.dlt");
    }
}

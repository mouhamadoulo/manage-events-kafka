package com.formation.eda.kafka.reliability;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.time.Duration;
import java.util.List;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ou part un message qui vient d'echouer. Une erreur de routage ici ne perd pas seulement le
 * message : elle le perd <b>silencieusement</b>, puisque le topic vise n'existe pas et que
 * {@code auto.create.topics.enable=false} fait echouer la publication du message qu'on
 * essayait justement de sauver.
 */
class DeadLetterRoutingTest {

    private static final String MAIN = "shop.order.events.v1";

    private static final ConsumerReliabilityProperties TWO_STAGES = new ConsumerReliabilityProperties(
            List.of(Duration.ofSeconds(5), Duration.ofSeconds(30)),
            2, Duration.ofMillis(200), 2.0, Duration.ofMillis(50));

    private final BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> resolver =
            ConsumerReliabilityConfiguration.destinationResolver(TWO_STAGES);

    @Test
    @DisplayName("un echec transitoire sur le topic principal part au premier etage")
    void aTransientFailureOnTheMainTopicGoesToTheFirstStage() {
        TopicPartition destination = resolver.apply(record(MAIN), transient_());

        assertThat(destination.topic()).isEqualTo("shop.order.events.v1.retry-1");
    }

    @Test
    @DisplayName("un echec transitoire sur le dernier etage part en dlt")
    void aTransientFailureOnTheLastStageGoesToTheDeadLetterTopic() {
        assertThat(resolver.apply(record(MAIN + ".retry-2"), transient_()).topic())
                .isEqualTo("shop.order.events.v1.dlt");
    }

    @Test
    @DisplayName("un echec definitif part en dlt depuis n'importe quel etage")
    void aFatalFailureGoesToTheDeadLetterTopicFromAnyStage() {
        // WHY: reessayer un echec definitif coute l'attente ET retarde le signalement du vrai
        // probleme de 35 secondes.
        assertThat(resolver.apply(record(MAIN), fatal()).topic())
                .isEqualTo("shop.order.events.v1.dlt");
        assertThat(resolver.apply(record(MAIN + ".retry-1"), fatal()).topic())
                .isEqualTo("shop.order.events.v1.dlt");
    }

    @Test
    @DisplayName("un echec de deserialisation part en dlt, jamais en retry")
    void aDeserializationFailureGoesToTheDeadLetterTopic() {
        Exception wrapped = new ListenerExecutionFailedException("echec",
                new DeserializationException("octet magique inconnu", new byte[]{1}, false, null));

        assertThat(resolver.apply(record(MAIN), wrapped).topic())
                .isEqualTo("shop.order.events.v1.dlt");
    }

    @Test
    @DisplayName("la partition n'est pas imposee : la cle decide")
    void thePartitionIsLeftToThePartitioner() {
        // Une partition negative dit au recoverer de ne pas en imposer une. Le partitionneur
        // la choisit depuis la cle, donc l'ordre par agregat est conserve a l'interieur d'un
        // etage. Imposer la partition d'origine casserait la repartition sur un .dlt a 1
        // partition.
        assertThat(resolver.apply(record(MAIN), transient_()).partition()).isNegative();
    }

    @Test
    @DisplayName("un record deja en dlt n'est pas renvoye vers un etage de retry")
    void aRecordAlreadyInTheDeadLetterTopicIsNotSentBackToARetryStage() {
        // Rien ne consomme la DLT aujourd'hui, mais un routage qui la renverrait en .retry-1
        // creerait une boucle : .dlt -> .retry-1 -> .retry-2 -> .dlt -> ...
        assertThat(resolver.apply(record(MAIN + ".dlt"), transient_()).topic())
                .isEqualTo("shop.order.events.v1.dlt");
    }

    @Test
    @DisplayName("avec un seul etage declare, le premier etage mene deja en dlt")
    void withOneStageDeclaredTheFirstStageAlreadyLeadsToTheDeadLetterTopic() {
        BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> oneStage =
                ConsumerReliabilityConfiguration.destinationResolver(new ConsumerReliabilityProperties(
                        List.of(Duration.ofSeconds(5)), 2,
                        Duration.ofMillis(200), 2.0, Duration.ofMillis(50)));

        assertThat(oneStage.apply(record(MAIN + ".retry-1"), transient_()).topic())
                .isEqualTo("shop.order.events.v1.dlt");
    }

    private static Exception transient_() {
        return new ListenerExecutionFailedException("echec",
                new TransientProcessingException("base injoignable", null));
    }

    private static Exception fatal() {
        return new ListenerExecutionFailedException("echec",
                new NonRetryableEventException("reference inconnue"));
    }

    private static ConsumerRecord<String, Object> record(String topic) {
        return new ConsumerRecord<>(topic, 2, 42L, "order-1", "payload");
    }
}

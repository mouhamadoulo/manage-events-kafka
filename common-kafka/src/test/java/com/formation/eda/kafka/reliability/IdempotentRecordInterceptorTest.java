package com.formation.eda.kafka.reliability;

import com.formation.eda.events.EventHeaders;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Kafka livre <b>au moins une fois</b>. Apres un rebalancing, un redemarrage ou un reset
 * d'offsets (272 evenements retraites en P1-T6), les memes evenements repassent. Ces tests
 * fixent ce que l'intercepteur doit laisser passer et ce qu'il doit sauter — y compris les cas
 * ou il ne doit surtout PAS dedupliquer, qui sont les plus faciles a rater.
 */
class IdempotentRecordInterceptorTest {

    private static final String TOPIC = "shop.order.events.v1";
    private static final String RETRY_1 = "shop.order.events.v1.retry-1";
    private static final String GROUP = "notification-service";

    private InMemoryProcessedEventStore store;
    private IdempotentRecordInterceptor interceptor;

    @BeforeEach
    void setUp() {
        store = new InMemoryProcessedEventStore();
        interceptor = new IdempotentRecordInterceptor(store);
    }

    @Test
    @DisplayName("un premier passage laisse le record passer")
    void firstDeliveryPasses() {
        ConsumerRecord<String, Object> record = record(TOPIC, UUID.randomUUID().toString());

        assertThat(interceptor.intercept(record, consumerOf(GROUP))).isSameAs(record);
    }

    @Test
    @DisplayName("un second passage du meme eventId est saute")
    void secondDeliveryOfSameEventIdIsSkipped() {
        String eventId = UUID.randomUUID().toString();
        Consumer<String, Object> consumer = consumerOf(GROUP);

        assertThat(interceptor.intercept(record(TOPIC, eventId), consumer)).isNotNull();
        assertThat(interceptor.intercept(record(TOPIC, eventId), consumer)).isNull();
    }

    @Test
    @DisplayName("un eventId identique dans un autre groupe de consumers passe")
    void sameEventIdInAnotherGroupPasses() {
        String eventId = UUID.randomUUID().toString();

        assertThat(interceptor.intercept(record(TOPIC, eventId), consumerOf(GROUP))).isNotNull();
        assertThat(interceptor.intercept(record(TOPIC, eventId), consumerOf("analytics-service"))).isNotNull();
    }

    @Test
    @DisplayName("un record venu de retry-1 est vu comme le meme evenement")
    void recordFromRetryTopicIsTheSameEvent() {
        String eventId = UUID.randomUUID().toString();
        Consumer<String, Object> consumer = consumerOf(GROUP);

        assertThat(interceptor.intercept(record(TOPIC, eventId), consumer)).isNotNull();
        // WHY ce test : si source_topic entrait dans la cle d'idempotence, chaque etage de
        // retry rejouerait l'effet metier.
        assertThat(interceptor.intercept(record(RETRY_1, eventId), consumer)).isNull();
    }

    @Test
    @DisplayName("un record sans header eventId passe sans deduplication")
    void recordWithoutEventIdHeaderPassesWithoutDeduplication() {
        ConsumerRecord<String, Object> record = record(TOPIC, null);

        assertThat(interceptor.intercept(record, consumerOf(GROUP))).isSameAs(record);
        assertThat(store.size()).isZero();
    }

    @Test
    @DisplayName("un eventId non parsable passe sans lever")
    void unparsableEventIdPassesWithoutThrowing() {
        ConsumerRecord<String, Object> record = record(TOPIC, "pas-un-uuid");

        // Une exception ici remonterait au conteneur AVANT tout classement d'erreur :
        // ni retry, ni DLT, et la partition bloquee.
        assertThat(interceptor.intercept(record, consumerOf(GROUP))).isSameAs(record);
        assertThat(store.size()).isZero();
    }

    @Test
    @DisplayName("un record a valeur nulle passe sans deduplication")
    void tombstonePassesWithoutDeduplication() {
        ConsumerRecord<String, Object> tombstone = new ConsumerRecord<>(
                TOPIC, 0, 0L, 0L, TimestampType.CREATE_TIME, -1, -1,
                "order-1", null, headersWith(UUID.randomUUID().toString()), Optional.empty());

        assertThat(interceptor.intercept(tombstone, consumerOf(GROUP))).isSameAs(tombstone);
        assertThat(store.size()).isZero();
    }

    @Test
    @DisplayName("failure retire la marque pour que le retry puisse retraiter")
    void failureRemovesTheMark() {
        String eventId = UUID.randomUUID().toString();
        Consumer<String, Object> consumer = consumerOf(GROUP);
        ConsumerRecord<String, Object> record = record(TOPIC, eventId);

        interceptor.intercept(record, consumer);
        interceptor.failure(record, new RuntimeException("echec du handler"), consumer);

        // Sans ce retrait, le retry serait classe doublon et le travail perdu en silence.
        assertThat(interceptor.intercept(record, consumer)).isNotNull();
    }

    @Test
    @DisplayName("un echec sur un record non deduplique ne retire pas la marque posee par un autre")
    void failureOnANonDeduplicatedRecordKeepsTheExistingMark() {
        String eventId = UUID.randomUUID().toString();
        Consumer<String, Object> consumer = consumerOf(GROUP);

        // Livraison normale : traitee, marque posee.
        interceptor.intercept(record(TOPIC, eventId), consumer);
        interceptor.afterRecord(record(TOPIC, eventId), consumer);

        // Copie illisible du MEME evenement : passe sans deduplication (payload nul), puis
        // echoue. Elle ne doit pas pouvoir effacer la marque d'un travail deja fait.
        ConsumerRecord<String, Object> poison = new ConsumerRecord<>(
                TOPIC, 0, 7L, 0L, TimestampType.CREATE_TIME, -1, -1,
                "order-1", null, headersWith(eventId), Optional.empty());
        interceptor.intercept(poison, consumer);
        interceptor.failure(poison, new RuntimeException("payload illisible"), consumer);

        assertThat(interceptor.intercept(record(TOPIC, eventId), consumer)).isNull();
    }

    @Test
    @DisplayName("un echec sur un doublon saute ne retire pas la marque")
    void failureOnASkippedDuplicateKeepsTheMark() {
        String eventId = UUID.randomUUID().toString();
        Consumer<String, Object> consumer = consumerOf(GROUP);

        interceptor.intercept(record(TOPIC, eventId), consumer);
        interceptor.afterRecord(record(TOPIC, eventId), consumer);

        ConsumerRecord<String, Object> duplicate = record(TOPIC, eventId);
        assertThat(interceptor.intercept(duplicate, consumer)).isNull();
        interceptor.failure(duplicate, new RuntimeException("ne devrait pas arriver"), consumer);

        assertThat(interceptor.intercept(record(TOPIC, eventId), consumer)).isNull();
    }

    private static ConsumerRecord<String, Object> record(String topic, String eventId) {
        return new ConsumerRecord<>(
                topic, 0, 0L, 0L, TimestampType.CREATE_TIME, -1, -1,
                "order-1", "payload", headersWith(eventId), Optional.empty());
    }

    private static Headers headersWith(String eventId) {
        Headers headers = new RecordHeaders();
        if (eventId != null) {
            headers.add(EventHeaders.EVENT_ID, eventId.getBytes(StandardCharsets.UTF_8));
        }
        return headers;
    }

    @SuppressWarnings("unchecked")
    private static Consumer<String, Object> consumerOf(String groupId) {
        Consumer<String, Object> consumer = mock(Consumer.class);
        // ConsumerGroupMetadata est depreciee pour suppression en kafka-clients 4.4 — ses DEUX
        // constructeurs. On la simule plutot que d'en appeler un, pour garder la sortie de
        // build propre.
        ConsumerGroupMetadata metadata = mock(ConsumerGroupMetadata.class);
        when(metadata.groupId()).thenReturn(groupId);
        when(consumer.groupMetadata()).thenReturn(metadata);
        return consumer;
    }

    /** Le vrai store est teste contre Postgres (JdbcProcessedEventStoreIT, tache 6). */
    private static final class InMemoryProcessedEventStore implements ProcessedEventStore {

        private final Set<String> marks = new HashSet<>();

        @Override
        public boolean markProcessed(UUID eventId, String consumerGroup, String sourceTopic) {
            return marks.add(eventId + "|" + consumerGroup);
        }

        @Override
        public void forget(UUID eventId, String consumerGroup) {
            marks.remove(eventId + "|" + consumerGroup);
        }

        int size() {
            return marks.size();
        }
    }
}

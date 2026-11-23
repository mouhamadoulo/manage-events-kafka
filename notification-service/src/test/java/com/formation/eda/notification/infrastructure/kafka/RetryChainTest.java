package com.formation.eda.notification.infrastructure.kafka;

import com.formation.eda.events.EventHeaders;
import com.formation.eda.events.Topics;
import com.formation.eda.events.order.OrderLine;
import com.formation.eda.events.order.OrderPlaced;
import com.formation.eda.kafka.json.EventJsonSchemaSerializer;
import com.formation.eda.kafka.json.SchemaCatalog;
import com.formation.eda.kafka.reliability.ProcessedEventStore;
import com.formation.eda.notification.application.EventCounters;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * La chaine complete, sur un broker embarque : topic principal, deux etages de retry, dead
 * letter topic, idempotence.
 *
 * <p>Les tests unitaires des taches 2 a 5 verifient chaque piece. Celui-ci verifie qu'elles
 * sont <b>cablees ensemble</b> — et surtout le piege central de la phase : qu'un record mis en
 * attente dans un etage de retry est bien traite au reveil, et <b>exactement une fois</b>.
 * C'est la propriete qu'une premiere version du differe cassait en silence.
 *
 * <p>Delais ramenes a quelques centaines de millisecondes : avec les 5 s et 30 s de production,
 * ce test durerait 40 secondes. Le rapport entre les etages est conserve.
 */
@SpringBootTest(properties = {
        // Flyway est le seul composant qui exigerait une base AU DEMARRAGE. Le registre
        // d'idempotence est remplace ci-dessous, donc aucune connexion n'est jamais ouverte.
        "spring.flyway.enabled=false",
        "spring.main.allow-bean-definition-overriding=true",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=mock://" + RetryChainTest.SCOPE,
        "spring.kafka.consumer.group-id=retry-chain-test",
        // Zero reessai bloquant : on veut que le premier echec parte tout de suite vers
        // l'etage 1, sinon le test mesurerait surtout le backoff bloquant.
        "eda.reliability.blocking-retries=0",
        "eda.reliability.stage-delays=400ms,900ms"
})
@EmbeddedKafka(
        topics = {Topics.ORDER_EVENTS, Topics.ORDER_EVENTS_RETRY_1,
                Topics.ORDER_EVENTS_RETRY_2, Topics.ORDER_EVENTS_DLT},
        partitions = 1)
@ActiveProfiles("lab")
class RetryChainTest {

    static final String SCOPE = "retry-chain";

    private static final List<String> TOPICS = List.of(
            Topics.ORDER_EVENTS, Topics.ORDER_EVENTS_RETRY_1,
            Topics.ORDER_EVENTS_RETRY_2, Topics.ORDER_EVENTS_DLT);

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private EventCounters counters;

    private KafkaTemplate<String, Object> producer;
    private Map<String, Long> baseline;

    @BeforeAll
    static void registerContracts() throws Exception {
        SchemaRegistryClient registry = MockSchemaRegistry.getClientForScope(SCOPE);
        SchemaCatalog catalog = new SchemaCatalog();
        // Un sujet PAR TOPIC : avec TopicRecordNameStrategy, le nom du topic fait partie du
        // sujet. Sans les sujets des topics derives, la publication en retry echouerait sur
        // « Error retrieving latest version » (piege trouve en P2-T3).
        for (String topic : TOPICS) {
            registry.register(topic + "-OrderPlaced", catalog.schemaFor("OrderPlaced"));
        }
    }

    @AfterAll
    static void dropRegistry() {
        MockSchemaRegistry.dropScope(SCOPE);
    }

    @BeforeEach
    void setUp() {
        counters.reset();
        producer = producer();
        // Les offsets s'accumulent d'un test a l'autre sur le meme broker : on raisonne en
        // ecarts, jamais en valeurs absolues.
        baseline = endOffsets();
    }

    @Test
    @DisplayName("un echec permanent traverse les deux etages puis atteint la dlt")
    void aPermanentFailureTraversesBothStagesThenReachesTheDeadLetterTopic() {
        publish(orderPlaced(), Map.of(EventHeaders.FAIL_TIMES, "9"));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(published(Topics.ORDER_EVENTS_DLT)).isEqualTo(1);
            assertThat(published(Topics.ORDER_EVENTS_RETRY_1)).isEqualTo(1);
            assertThat(published(Topics.ORDER_EVENTS_RETRY_2)).isEqualTo(1);
        });
        // Aucun effet metier : le handler a echoue a chaque passage.
        assertThat(counters.receivedCount()).isZero();
    }

    @Test
    @DisplayName("un record pas encore du est traite apres la pause, une seule fois")
    void anUndueRecordIsProcessedAfterThePauseExactlyOnce() {
        // LE test du piege : un seul echec, donc un seul etage. Le record arrive dans
        // .retry-1 avant son echeance, sa partition est mise en pause, puis il est redelivre.
        // Si l'intercepteur d'idempotence avait garde sa marque pendant l'attente, le record
        // serait classe doublon au reveil et le compteur resterait a ZERO — perte silencieuse.
        publish(orderPlaced(), Map.of(EventHeaders.FAIL_TIMES, "1"));

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(counters.receivedCount()).isEqualTo(1));

        assertThat(published(Topics.ORDER_EVENTS_RETRY_1)).isEqualTo(1);
        assertThat(published(Topics.ORDER_EVENTS_RETRY_2)).isZero();
        assertThat(published(Topics.ORDER_EVENTS_DLT)).isZero();
        // Et pas deux fois non plus : l'attente ne doit pas compter comme un traitement.
        assertThat(counters.receivedCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("un echec non retryable va directement en dlt, sans toucher aux etages")
    void aNonRetryableFailureGoesStraightToTheDeadLetterTopic() {
        publish(orderPlaced(), Map.of(EventHeaders.FAIL_FATAL, "true"));

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(published(Topics.ORDER_EVENTS_DLT)).isEqualTo(1));

        assertThat(published(Topics.ORDER_EVENTS_RETRY_1)).isZero();
        assertThat(published(Topics.ORDER_EVENTS_RETRY_2)).isZero();
        assertThat(counters.receivedCount()).isZero();
    }

    @Test
    @DisplayName("deux livraisons du meme eventId ne produisent qu'un effet")
    void twoDeliveriesOfTheSameEventIdProduceASingleEffect() {
        OrderPlaced event = orderPlaced();

        publish(event, Map.of());
        publish(event, Map.of());

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(counters.receivedCount()).isEqualTo(1));

        // Les DEUX records sont bien dans le topic : c'est le consumer qui deduplique, pas le
        // producteur. L'idempotence producteur de la phase 3 ne couvre pas ce cas.
        assertThat(published(Topics.ORDER_EVENTS)).isEqualTo(2);
        assertThat(published(Topics.ORDER_EVENTS_DLT)).isZero();
    }

    private void publish(OrderPlaced event, Map<String, String> headers) {
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(Topics.ORDER_EVENTS, null, event.partitionKey(), event);
        record.headers().add(EventHeaders.EVENT_ID,
                event.envelope().eventId().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(EventHeaders.EVENT_TYPE,
                event.envelope().eventType().getBytes(StandardCharsets.UTF_8));
        headers.forEach((name, value) ->
                record.headers().add(name, value.getBytes(StandardCharsets.UTF_8)));
        producer.send(record).join();
    }

    /** Nombre de records publies sur ce topic DEPUIS le debut du test. */
    private long published(String topic) {
        return endOffsets().get(topic) - baseline.get(topic);
    }

    private Map<String, Long> endOffsets() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", broker.getBrokersAsString());
        configs.put("key.deserializer", StringDeserializer.class);
        configs.put("value.deserializer", ByteArrayDeserializer.class);
        // Pas de group.id : cette lecture ne commite rien et ne perturbe pas le groupe du
        // service.
        try (Consumer<String, byte[]> consumer = new KafkaConsumer<>(configs)) {
            Set<TopicPartition> partitions = new HashSet<>();
            TOPICS.forEach(topic -> partitions.add(new TopicPartition(topic, 0)));
            Map<String, Long> offsets = new HashMap<>();
            consumer.endOffsets(partitions)
                    .forEach((partition, offset) -> offsets.put(partition.topic(), offset));
            return offsets;
        }
    }

    private KafkaTemplate<String, Object> producer() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EventJsonSchemaSerializer.class);
        configs.put("schema.registry.url", "mock://" + SCOPE);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(configs));
    }

    private static OrderPlaced orderPlaced() {
        return OrderPlaced.of("order-" + UUID.randomUUID(), "customer-1",
                List.of(new OrderLine("SKU-1", 1, new BigDecimal("12.50"))), "EUR");
    }

    /**
     * Registre d'idempotence en memoire : ce test porte sur la chaine Kafka, pas sur le SQL.
     * Le vrai store est verifie contre un Postgres reel par
     * {@code JdbcProcessedEventStoreIT}.
     */
    @TestConfiguration
    static class InMemoryStoreConfiguration {

        @Bean
        ProcessedEventStore processedEventStore() {
            Set<String> marks = java.util.concurrent.ConcurrentHashMap.newKeySet();
            return new ProcessedEventStore() {
                @Override
                public boolean markProcessed(UUID eventId, String consumerGroup, String sourceTopic) {
                    return marks.add(eventId + "|" + consumerGroup);
                }

                @Override
                public void forget(UUID eventId, String consumerGroup) {
                    marks.remove(eventId + "|" + consumerGroup);
                }
            };
        }
    }
}

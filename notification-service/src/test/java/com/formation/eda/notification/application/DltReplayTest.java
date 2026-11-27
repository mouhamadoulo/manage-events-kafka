package com.formation.eda.notification.application;

import com.formation.eda.events.EventHeaders;
import com.formation.eda.events.Topics;
import com.formation.eda.events.order.OrderLine;
import com.formation.eda.events.order.OrderPlaced;
import com.formation.eda.kafka.json.EventJsonSchemaSerializer;
import com.formation.eda.kafka.json.SchemaCatalog;
import com.formation.eda.kafka.reliability.ProcessedEventStore;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Inspection et rejeu selectif du dead letter topic.
 *
 * <p>La propriete que ces tests fixent n'est pas « un rejeu est saute » — ce serait faux, et le
 * rejeu ne servirait a rien. Un message arrive en DLT apres un echec a vu sa marque
 * d'idempotence <b>effacee</b> : le rejeu le <b>retraite</b>, et c'est le but. La propriete
 * vraie est : <b>rejouer deux fois ne produit qu'un effet.</b>
 */
@SpringBootTest(properties = {
        "spring.flyway.enabled=false",
        "spring.main.allow-bean-definition-overriding=true",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=mock://" + DltReplayTest.SCOPE,
        "spring.kafka.consumer.group-id=dlt-replay-test"
})
@EmbeddedKafka(
        topics = {Topics.ORDER_EVENTS, Topics.ORDER_EVENTS_RETRY_1,
                Topics.ORDER_EVENTS_RETRY_2, Topics.ORDER_EVENTS_DLT},
        partitions = 1)
@ActiveProfiles("lab")
class DltReplayTest {

    static final String SCOPE = "dlt-replay";

    @Autowired
    private DltReplayService replayService;

    @Autowired
    private EventCounters counters;

    @Autowired
    private org.springframework.kafka.test.EmbeddedKafkaBroker broker;

    @Autowired
    @Qualifier("byteArrayDltTemplate")
    private KafkaTemplate<String, byte[]> dltTemplate;

    private static SchemaRegistryClient registry;

    @BeforeAll
    static void registerContracts() throws Exception {
        registry = MockSchemaRegistry.getClientForScope(SCOPE);
        SchemaCatalog catalog = new SchemaCatalog();
        for (String topic : List.of(Topics.ORDER_EVENTS, Topics.ORDER_EVENTS_RETRY_1,
                Topics.ORDER_EVENTS_RETRY_2, Topics.ORDER_EVENTS_DLT)) {
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
    }

    @Test
    @DisplayName("peek montre le topic d'origine et la cause, sans consommer")
    void peekShowsTheOriginTopicAndTheCause() {
        OrderPlaced event = orderPlaced();
        long offset = putInDeadLetterTopic(event, Topics.ORDER_EVENTS, "panne simulee");

        List<DltReplayService.DltRecordView> view =
                replayService.peek(Topics.ORDER_EVENTS_DLT, 20);

        assertThat(view).anySatisfy(record -> {
            assertThat(record.offset()).isEqualTo(offset);
            assertThat(record.key()).isEqualTo(event.partitionKey());
            assertThat(record.originTopic()).isEqualTo(Topics.ORDER_EVENTS);
            assertThat(record.exceptionMessage()).contains("panne simulee");
        });
    }

    @Test
    @DisplayName("un rejeu republie sur le topic d'origine et le message est traite")
    void aReplayRepublishesToTheOriginTopicAndTheMessageIsProcessed() {
        OrderPlaced event = orderPlaced();
        long offset = putInDeadLetterTopic(event, Topics.ORDER_EVENTS, "panne simulee");

        DltReplayService.ReplayReport report = replayService.replay(
                new DltReplayService.ReplayRequest(Topics.ORDER_EVENTS_DLT, 0, List.of(offset)));

        assertThat(report.republished()).isEqualTo(1);
        assertThat(report.rejected()).isEmpty();
        assertThat(report.notFound()).isEmpty();

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(counters.receivedCount()).isEqualTo(1));
    }

    @Test
    @DisplayName("deux rejeux du meme offset ne produisent qu'un effet")
    void twoReplaysOfTheSameOffsetProduceASingleEffect() {
        OrderPlaced event = orderPlaced();
        long offset = putInDeadLetterTopic(event, Topics.ORDER_EVENTS, "panne simulee");
        DltReplayService.ReplayRequest request =
                new DltReplayService.ReplayRequest(Topics.ORDER_EVENTS_DLT, 0, List.of(offset));

        assertThat(replayService.replay(request).republished()).isEqualTo(1);
        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(counters.receivedCount()).isEqualTo(1));

        // Le second rejeu republie bien le message — c'est le CONSUMER qui le saute, grace a
        // processed_event. L'endpoint n'a pas a tenir d'historique de ses propres appels.
        assertThat(replayService.replay(request).republished()).isEqualTo(1);

        await().during(Duration.ofSeconds(3))
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(counters.receivedCount()).isEqualTo(1));
    }

    @Test
    @DisplayName("un record sans x-origin-topic est rejete, jamais republie")
    void aRecordWithoutOriginTopicIsRejected() {
        // Sans ce garde-fou, le rejeu republie sur .dlt lui-meme : une boucle infinie
        // alimentee par l'operateur, un message de plus a chaque appel.
        Headers headers = new RecordHeaders();
        headers.add(EventHeaders.EVENT_ID,
                UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        long offset = send(Topics.ORDER_EVENTS_DLT, "order-sans-origine", new byte[]{1, 2, 3}, headers);

        DltReplayService.ReplayReport report = replayService.replay(
                new DltReplayService.ReplayRequest(Topics.ORDER_EVENTS_DLT, 0, List.of(offset)));

        assertThat(report.rejected()).containsExactly(offset);
        assertThat(report.republished()).isZero();
    }

    @Test
    @DisplayName("un offset au-dela de la fin du log est signale, sans blocage")
    void anOffsetBeyondTheEndOfTheLogIsReported() {
        // Sans lecture prealable des endOffsets, poll() tournerait jusqu'au timeout du client
        // HTTP pour un offset qui n'existera jamais.
        DltReplayService.ReplayReport report = replayService.replay(
                new DltReplayService.ReplayRequest(Topics.ORDER_EVENTS_DLT, 0, List.of(999_999L)));

        assertThat(report.notFound()).containsExactly(999_999L);
        assertThat(report.republished()).isZero();
    }

    @Test
    @DisplayName("un offset nul dans la liste est refuse, pas une NPE")
    void aNullOffsetInTheListIsRejected() {
        // Trouve en verification reelle : un client qui envoie offsets:[null] — ce que fait
        // PowerShell quand la variable attendue est vide — provoquait
        // « Cannot invoke java.lang.Comparable.compareTo(Object) because k1 is null », donc
        // un 500 la ou le client a envoye une requete invalide.
        List<Long> withNull = new java.util.ArrayList<>();
        withNull.add(null);

        assertThatThrownBy(() -> replayService.replay(
                new DltReplayService.ReplayRequest(Topics.ORDER_EVENTS_DLT, 0, withNull)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nul");
    }

    @Test
    @DisplayName("une liste d'offsets vide est refusee")
    void anEmptyOffsetListIsRejected() {
        assertThatThrownBy(() -> replayService.replay(
                new DltReplayService.ReplayRequest(Topics.ORDER_EVENTS_DLT, 0, List.of())))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("rejouer un offset ancien ne lit pas tout le reste du log")
    void replayingAnOldOffsetDoesNotReadTheWholeTailOfTheLog() {
        // Le javadoc du controleur annonce « un appel ne peut pas faire tourner la memoire du
        // service ». La borne de 100 offsets ne garantissait rien : la boucle de lecture
        // s'arretait a la FIN DU LOG, pas a l'offset cherche. Rejouer l'offset 5 d'une DLT qui
        // en contient 50 000 chargeait 50 000 records.
        long premier = putInDeadLetterTopic(orderPlaced(), Topics.ORDER_EVENTS, "panne simulee");
        for (int i = 0; i < 40; i++) {
            putInDeadLetterTopic(orderPlaced(), Topics.ORDER_EVENTS, "bruit " + i);
        }

        long avant = System.currentTimeMillis();
        DltReplayService.ReplayReport report = replayService.replay(
                new DltReplayService.ReplayRequest(Topics.ORDER_EVENTS_DLT, 0, List.of(premier)));
        long duree = System.currentTimeMillis() - avant;

        assertThat(report.republished()).isEqualTo(1);
        assertThat(report.scanned())
                .as("records lus pour un seul offset ancien")
                .isLessThanOrEqualTo(10);
        assertThat(duree).as("duree de lecture, ms").isLessThan(5_000L);
    }

    @Test
    @DisplayName("plusieurs offsets disperses sont lus en une seule passe")
    void severalScatteredOffsetsAreReadInASingleScan() {
        List<Long> offsets = new java.util.ArrayList<>();
        for (int i = 0; i < 12; i++) {
            long offset = putInDeadLetterTopic(orderPlaced(), Topics.ORDER_EVENTS, "cible " + i);
            if (i % 4 == 0) {
                offsets.add(offset);
            }
        }

        DltReplayService.ReplayReport report = replayService.replay(
                new DltReplayService.ReplayRequest(Topics.ORDER_EVENTS_DLT, 0, offsets));

        assertThat(report.republished()).isEqualTo(offsets.size());
        // Une passe unique : on ne relit pas le log une fois par offset demande.
        assertThat(report.scanned()).isLessThanOrEqualTo(20);
    }

    @Test
    @DisplayName("les deux bouts d'une longue plage sont rejoues sans accumuler la plage")
    void bothEndsOfALongSpanAreReplayedWithoutAccumulatingTheSpan() {
        // La premiere correction bornait la PLAGE lue, pas la MEMOIRE : les records de `first` a
        // `last` etaient accumules dans une liste avant d'etre filtres. Demander les offsets 0
        // et 49 999 chargeait donc 50 000 records — la borne de 100 offsets ne protegeait
        // toujours rien.
        //
        // La lecture est desormais STREAMING : chaque record est teste puis relache. Ce test ne
        // peut pas observer l'allocation ; il garantit la correction du parcours, qui est ce
        // qu'un refactor en streaming risque de casser (sortie anticipee, offsets manques).
        long premier = putInDeadLetterTopic(orderPlaced(), Topics.ORDER_EVENTS, "cible debut");
        long dernier = premier;
        for (int i = 0; i < 40; i++) {
            dernier = putInDeadLetterTopic(orderPlaced(), Topics.ORDER_EVENTS, "bruit " + i);
        }

        DltReplayService.ReplayReport report = replayService.replay(
                new DltReplayService.ReplayRequest(
                        Topics.ORDER_EVENTS_DLT, 0, List.of(premier, dernier)));

        assertThat(report.republished()).isEqualTo(2);
        assertThat(report.notFound()).isEmpty();
        assertThat(report.rejected()).isEmpty();
    }

    @Test
    @DisplayName("le rejeu s'arrete des que tous les offsets demandes sont trouves")
    void theReplayStopsAsSoonAsEveryRequestedOffsetIsFound() {
        long premier = putInDeadLetterTopic(orderPlaced(), Topics.ORDER_EVENTS, "cible 1");
        long second = putInDeadLetterTopic(orderPlaced(), Topics.ORDER_EVENTS, "cible 2");
        for (int i = 0; i < 40; i++) {
            putInDeadLetterTopic(orderPlaced(), Topics.ORDER_EVENTS, "bruit " + i);
        }

        DltReplayService.ReplayReport report = replayService.replay(
                new DltReplayService.ReplayRequest(
                        Topics.ORDER_EVENTS_DLT, 0, List.of(premier, second)));

        assertThat(report.republished()).isEqualTo(2);
        assertThat(report.scanned())
                .as("records lus pour deux offsets voisins au debut d'une longue plage")
                .isLessThanOrEqualTo(10);
    }

    @Test
    @DisplayName("une partition inconnue est refusee tout de suite, pas apres 60 s")
    void anUnknownPartitionIsRejectedImmediately() {
        // endOffsets() sur une partition inexistante attend default.api.timeout.ms (60 s) :
        // le thread HTTP bloquait une minute puis rendait 500.
        long avant = System.currentTimeMillis();

        assertThatThrownBy(() -> replayService.replay(
                new DltReplayService.ReplayRequest(Topics.ORDER_EVENTS_DLT, 977, List.of(0L))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("977");

        assertThat(System.currentTimeMillis() - avant).isLessThan(10_000L);
    }

    @Test
    @DisplayName("un x-origin-topic qui pointe une dlt est refuse")
    void anOriginTopicPointingAtADeadLetterTopicIsRejected() {
        // Le garde-fou verifiait la PRESENCE du header, pas sa valeur. Un x-origin-topic valant
        // la dlt elle-meme faisait republier dedans : un message de plus a chaque appel, la
        // meme boucle infinie que le header absent.
        long offset = putInDeadLetterTopic(orderPlaced(), Topics.ORDER_EVENTS_DLT, "panne simulee");

        DltReplayService.ReplayReport report = replayService.replay(
                new DltReplayService.ReplayRequest(Topics.ORDER_EVENTS_DLT, 0, List.of(offset)));

        assertThat(report.rejected()).containsExactly(offset);
        assertThat(report.republished()).isZero();
    }

    @Test
    @DisplayName("un x-origin-topic qui pointe un etage de retry est refuse")
    void anOriginTopicPointingAtARetryStageIsRejected() {
        long offset = putInDeadLetterTopic(orderPlaced(), Topics.ORDER_EVENTS_RETRY_1, "panne simulee");

        DltReplayService.ReplayReport report = replayService.replay(
                new DltReplayService.ReplayRequest(Topics.ORDER_EVENTS_DLT, 0, List.of(offset)));

        assertThat(report.rejected()).containsExactly(offset);
    }

    @Test
    @DisplayName("la vue d'un message en dlt porte sa partition")
    void theViewCarriesItsPartition() {
        // Sans elle, un operateur lit « offset=23 » et doit deviner la partition pour le
        // rejeu. Sur une dlt a plusieurs partitions, l'offset 23 existe dans chacune : le
        // rejeu republierait un AUTRE message que celui qu'on a regarde.
        long offset = putInDeadLetterTopic(orderPlaced(), Topics.ORDER_EVENTS, "panne simulee");

        assertThat(replayService.peek(Topics.ORDER_EVENTS_DLT, 20))
                .anySatisfy(record -> {
                    assertThat(record.offset()).isEqualTo(offset);
                    assertThat(record.partition()).isZero();
                });
    }

    @Test
    @DisplayName("plus de 100 offsets est refuse")
    void moreThanOneHundredOffsetsIsRejected() {
        List<Long> tooMany = LongStream.range(0, 101).boxed().toList();

        assertThatThrownBy(() -> replayService.replay(
                new DltReplayService.ReplayRequest(Topics.ORDER_EVENTS_DLT, 0, tooMany)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("100");
    }

    @Test
    @DisplayName("le rejeu ne cree aucun groupe de consumers cote broker")
    void theReplayCreatesNoConsumerGroup() throws Exception {
        OrderPlaced event = orderPlaced();
        long offset = putInDeadLetterTopic(event, Topics.ORDER_EVENTS, "panne simulee");

        replayService.replay(
                new DltReplayService.ReplayRequest(Topics.ORDER_EVENTS_DLT, 0, List.of(offset)));

        // assign + seek sans group.id : aucun offset commite, donc aucune trace cote broker, et
        // surtout aucun risque de perturber le groupe du service. Verifie aupres du broker,
        // pas par un accesseur de test ajoute au code de production.
        try (org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(
                java.util.Map.of("bootstrap.servers", broker.getBrokersAsString()))) {
            List<String> groups = admin.listGroups().all().get().stream()
                    .map(org.apache.kafka.clients.admin.GroupListing::groupId)
                    .toList();
            assertThat(groups).containsExactly("dlt-replay-test");
        }
    }

    /** Place un message VALIDE dans la DLT, avec les headers qu'un vrai echec y aurait poses. */
    private long putInDeadLetterTopic(OrderPlaced event, String originTopic, String cause) {
        byte[] payload;
        try (EventJsonSchemaSerializer serializer = new EventJsonSchemaSerializer()) {
            serializer.configure(java.util.Map.of("schema.registry.url", "mock://" + SCOPE), false);
            // Serialise POUR LE TOPIC DE DESTINATION : avec TopicRecordNameStrategy, le sujet
            // depend du topic. Les octets restent valides sur le topic metier parce que l'id de
            // schema inscrit dans le message est GLOBAL (verifie en P2-T3).
            payload = serializer.serialize(Topics.ORDER_EVENTS_DLT, event);
        }

        Headers headers = new RecordHeaders();
        headers.add(EventHeaders.EVENT_ID,
                event.envelope().eventId().toString().getBytes(StandardCharsets.UTF_8));
        headers.add(EventHeaders.EVENT_TYPE,
                event.envelope().eventType().getBytes(StandardCharsets.UTF_8));
        headers.add(EventHeaders.CORRELATION_ID,
                event.envelope().correlationId().getBytes(StandardCharsets.UTF_8));
        headers.add(EventHeaders.ORIGIN_TOPIC, originTopic.getBytes(StandardCharsets.UTF_8));
        headers.add("kafka_dlt-exception-message", cause.getBytes(StandardCharsets.UTF_8));
        headers.add("kafka_dlt-exception-fqcn",
                "com.formation.eda.kafka.reliability.TransientProcessingException"
                        .getBytes(StandardCharsets.UTF_8));

        return send(Topics.ORDER_EVENTS_DLT, event.partitionKey(), payload, headers);
    }

    private long send(String topic, String key, byte[] value, Headers headers) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, null, key, value);
        headers.forEach(header -> record.headers().add(header));
        return dltTemplate.send(record).join().getRecordMetadata().offset();
    }

    private static OrderPlaced orderPlaced() {
        return OrderPlaced.of("order-" + UUID.randomUUID(), "customer-9",
                List.of(new OrderLine("SKU-9", 3, new BigDecimal("5.00"))), "EUR");
    }

    @TestConfiguration
    static class InMemoryStoreConfiguration {

        @Bean
        ProcessedEventStore processedEventStore() {
            Set<String> marks = ConcurrentHashMap.newKeySet();
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

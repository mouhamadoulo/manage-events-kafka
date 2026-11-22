package com.formation.eda.kafka.json;

import com.formation.eda.events.order.OrderLine;
import com.formation.eda.events.order.OrderPlaced;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Contrat de serialisation de la phase 2. Tourne sur un Schema Registry en memoire
 * ({@code mock://}) : pas besoin de l'infra Docker pour ces tests.
 */
class EventJsonSchemaSerdeTest {

    private static final String SCOPE = "phase2-serde";
    private static final String REGISTRY_URL = "mock://" + SCOPE;
    private static final String TOPIC = "shop.order.events.v1";
    private static final String SUBJECT = TOPIC + "-OrderPlaced";

    private final SchemaCatalog catalog = new SchemaCatalog();
    private SchemaRegistryClient registry;

    @BeforeEach
    void registerContract() throws Exception {
        registry = MockSchemaRegistry.getClientForScope(SCOPE);
        // Enregistrement explicite, comme le fera infra/schemas/register-schemas.ps1 :
        // auto.register.schemas=false cote producteur.
        registry.register(SUBJECT, catalog.schemaFor("OrderPlaced"));
    }

    @AfterEach
    void clean() {
        MockSchemaRegistry.dropScope(SCOPE);
    }

    private EventJsonSchemaSerializer serializer() {
        EventJsonSchemaSerializer serializer = new EventJsonSchemaSerializer();
        serializer.configure(Map.of("schema.registry.url", REGISTRY_URL), false);
        return serializer;
    }

    private static OrderPlaced orderPlaced(String currency) {
        return OrderPlaced.of("order-1", "customer-7",
                List.of(new OrderLine("SKU-1", 2, new BigDecimal("19.99"))), currency);
    }

    @Test
    @DisplayName("le message porte l'octet magique 0, l'id du schema, puis le JSON")
    void wireFormatCarriesTheSchemaId() throws Exception {
        int expectedId = registry.getLatestSchemaMetadata(SUBJECT).getId();

        byte[] bytes;
        try (EventJsonSchemaSerializer serializer = serializer()) {
            bytes = serializer.serialize(TOPIC, orderPlaced("EUR"));
        }

        // 1 octet magique + 4 octets d'id : c'est tout ce qui separe ce format d'un JSON nu.
        // Le consumer y lit QUEL schema a servi, au lieu de le deviner.
        assertThat(bytes[0]).isZero();
        assertThat(ByteBuffer.wrap(bytes, 1, 4).getInt()).isEqualTo(expectedId);

        String payload = new String(bytes, 5, bytes.length - 5, StandardCharsets.UTF_8);
        assertThat(payload).contains("\"eventType\":\"OrderPlaced\"");
        // ISO-8601 et non timestamp decimal : c'est tout l'objet du reglage des dates.
        assertThat(payload).containsPattern("\"occurredAt\":\"\\d{4}-\\d{2}-\\d{2}T");
        assertThat(payload).doesNotContain("\"root\"");
    }

    @Test
    @DisplayName("aller-retour sans perte, Instant a la nanoseconde")
    void roundTripsThroughTheRegistry() {
        OrderPlaced original = orderPlaced("EUR");

        byte[] bytes;
        try (EventJsonSchemaSerializer serializer = serializer()) {
            bytes = serializer.serialize(TOPIC, original);
        }

        try (KafkaJsonSchemaDeserializer<OrderPlaced> deserializer = new KafkaJsonSchemaDeserializer<>()) {
            deserializer.configure(Map.of(
                    "schema.registry.url", REGISTRY_URL,
                    "json.value.type", OrderPlaced.class.getName()), false);
            OrderPlaced reread = deserializer.deserialize(TOPIC, bytes);

            assertThat(reread).isEqualTo(original);
            assertThat(reread.envelope().occurredAt()).isEqualTo(original.envelope().occurredAt());
        }
    }

    @Test
    @DisplayName("le schema refuse ce que le record accepte : currency en minuscules")
    void schemaRejectsWhatTheRecordCannotExpress() {
        // Le record exige seulement une chaine non vide. Le schema exige ^[A-Z]{3}$.
        // Deux filets a des mailles differentes, et c'est voulu : le contrat reseau peut etre
        // plus strict que le type Java, et il s'applique aussi aux producteurs non-Java.
        OrderPlaced suspicious = orderPlaced("eur");

        try (EventJsonSchemaSerializer serializer = serializer()) {
            assertThatThrownBy(() -> serializer.serialize(TOPIC, suspicious))
                    .hasMessageContaining("Error serializing JSON message");
        }
    }

    @Test
    @DisplayName("publier un evenement sur un topic qui n'est pas le sien echoue")
    void refusesAForeignTopic() {
        try (EventJsonSchemaSerializer serializer = serializer()) {
            assertThatThrownBy(() -> serializer.serialize("shop.payment.events.v1", orderPlaced("EUR")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("declare sur shop.order.events.v1");
        }
    }

    @Test
    @DisplayName("un topic derive a son propre sujet : sans enregistrement, la publication echoue")
    void derivedTopicNeedsItsOwnSubject() {
        // Consequence directe de TopicRecordNameStrategy : le sujet contient le nom du topic.
        // La DLT de la phase 4 portera le meme type d'evenement, mais sous le sujet
        // "shop.order.events.v1.dlt-OrderPlaced". Il doit donc etre enregistre lui aussi —
        // c'est ce que fait infra/schemas/register-schemas.ps1 pour les topics retry et DLT.
        try (EventJsonSchemaSerializer serializer = serializer()) {
            assertThatThrownBy(() -> serializer.serialize(TOPIC + ".dlt", orderPlaced("EUR")))
                    .hasMessageContaining("Error retrieving latest version");
        }
    }

    @Test
    @DisplayName("une fois son sujet enregistre, le topic derive accepte le meme evenement")
    void derivedTopicWorksOnceRegistered() throws Exception {
        registry.register(TOPIC + ".dlt-OrderPlaced", catalog.schemaFor("OrderPlaced"));

        try (EventJsonSchemaSerializer serializer = serializer()) {
            assertThat(serializer.serialize(TOPIC + ".dlt", orderPlaced("EUR"))).isNotEmpty();
        }
    }

    @Test
    @DisplayName("un tombstone reste un null")
    void nullStaysNull() {
        try (EventJsonSchemaSerializer serializer = serializer()) {
            assertThat(serializer.serialize(TOPIC, null)).isNull();
        }
    }

    @Test
    @DisplayName("chaque type declare a un schema, dont le titre est le type")
    void everyDeclaredEventTypeHasAMatchingSchema() {
        assertThat(catalog.declaredEventTypes()).contains("OrderPlaced");

        for (String eventType : catalog.declaredEventTypes()) {
            assertThat(catalog.schemaFor(eventType).name())
                    .as("le titre du schema de %s doit etre son eventType", eventType)
                    .isEqualTo(eventType);
            assertThat(catalog.subjectFor(eventType)).endsWith("-" + eventType);
        }
    }
}

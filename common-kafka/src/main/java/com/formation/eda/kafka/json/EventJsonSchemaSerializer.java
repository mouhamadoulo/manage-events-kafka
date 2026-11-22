package com.formation.eda.kafka.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.formation.eda.events.DomainEvent;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Serialiseur des evenements du projet : JSON Schema + Confluent Schema Registry.
 *
 * <p>Il enveloppe {@link KafkaJsonSchemaSerializer} pour imposer trois choses que la
 * configuration par defaut ne donne pas :
 *
 * <ol>
 *   <li><b>le schema vient du fichier, pas de la classe.</b> Le schema derive de
 *       {@code OrderPlaced} s'appellerait {@code "Order Placed"} — avec une espace, donc un
 *       sujet {@code shop.order.events.v1-Order Placed} — et perdrait toutes les contraintes
 *       metier. On passe donc le schema explicitement par le mecanisme d'enveloppe de
 *       Confluent ({@link JsonSchemaUtils#envelope}) ;</li>
 *   <li><b>les dates en ISO-8601.</b> Les serdes Confluent 8.4.1 fonctionnent en <b>Jackson
 *       2</b>, qui ecrit un {@code Instant} sous forme de timestamp decimal
 *       ({@code 1795344536.104638100}) tant que le module java.time n'est pas enregistre.
 *       Le payload de la phase 1, produit par Jackson 3, etait en ISO-8601 : sans ce reglage,
 *       le passage au Schema Registry changeait silencieusement le format d'un champ du
 *       contrat ;</li>
 *   <li><b>la validation du payload.</b> {@code json.fail.invalid.schema} vaut {@code false}
 *       par defaut : un message hors contrat partirait sans un mot.</li>
 * </ol>
 *
 * <p><b>A savoir</b> : cette validation est faite par le <b>client producteur</b>. Le broker
 * Apache Kafka ne lit jamais les messages et le Schema Registry ne voit que les schemas — il
 * verifie la compatibilite entre versions, pas la conformite des messages. Rien n'empeche un
 * producteur mal configure d'ecrire n'importe quoi dans un topic.
 */
public class EventJsonSchemaSerializer implements Serializer<DomainEvent> {

    /** Reglage de laboratoire : voir {@link #configure(Map, boolean)}. */
    public static final String VALIDATE_PAYLOAD = "eda.validate.payload";

    private final SchemaCatalog catalog = new SchemaCatalog();

    // Jackson 2, comme les serdes Confluent. Ce mapper decide du format du payload, puisque
    // l'evenement est converti en JsonNode ici avant d'etre passe au delegue.
    private final ObjectMapper jackson2 = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private KafkaJsonSchemaSerializer<Object> delegate;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> merged = new HashMap<>(configs);

        // auto.register.schemas=false : un schema n'arrive pas dans le registre par effet de
        // bord d'un deploiement. L'enregistrement est un acte explicite et revu
        // (infra/schemas/register-schemas.ps1).
        merged.put("auto.register.schemas", false);
        // Le producteur utilise la derniere version enregistree du sujet comme reference.
        merged.put("use.latest.version", true);
        // Sans ce false, le serialiseur exigerait que le schema fourni soit compatible avec
        // la derniere version selon SES regles de derivation. Ici le fichier EST la verite.
        merged.put("latest.compatibility.strict", false);
        // sujet = <topic>-<titre du schema>. WHY: un topic metier transporte plusieurs types
        // d'evenements des la phase 6 ; TopicNameStrategy (<topic>-value) ne permettrait
        // qu'un seul schema par topic.
        merged.put(isKey ? "key.subject.name.strategy" : "value.subject.name.strategy",
                TopicRecordNameStrategy.class.getName());
        // Validation du payload contre le schema, a chaque message. Active par defaut : sans
        // elle, un message hors contrat part sans un mot (le defaut Confluent est false).
        //
        // La cle VALIDATE_PAYLOAD permet de la couper, et elle n'existe que pour pouvoir
        // MESURER ce qu'elle coute (experience P3-T5) : la validation JSON Schema se fait sur
        // le thread appelant, a chaque envoi, et devient vite le facteur limitant du debit.
        // Aucun service ne la coupe — seul l'outil de rafale du profil lab le fait.
        Object validate = merged.getOrDefault(VALIDATE_PAYLOAD, Boolean.TRUE);
        merged.put("json.fail.invalid.schema", Boolean.parseBoolean(String.valueOf(validate)));
        merged.remove(VALIDATE_PAYLOAD);

        delegate = new KafkaJsonSchemaSerializer<>();
        delegate.configure(merged, isKey);
    }

    @Override
    public byte[] serialize(String topic, DomainEvent event) {
        if (event == null) {
            // Un tombstone est legitime sur un topic compacte : il doit rester un null,
            // pas devenir un message "null" serialise.
            return null;
        }
        String eventType = event.envelope().eventType();
        requireDeclaredTopic(topic, eventType);

        JsonSchema schema = catalog.schemaFor(eventType);
        JsonNode payload = jackson2.valueToTree(event);
        return delegate.serialize(topic, JsonSchemaUtils.envelope(schema, payload));
    }

    @Override
    public byte[] serialize(String topic, Headers headers, DomainEvent event) {
        return serialize(topic, event);
    }

    /**
     * Garde-fou : publier un evenement sur un topic qui n'est pas le sien est un bug, pas une
     * souplesse. Les topics derives (retry, DLT, introduits en phase 4) sont acceptes.
     */
    private void requireDeclaredTopic(String topic, String eventType) {
        String declared = catalog.topicFor(eventType);
        if (!topic.equals(declared) && !topic.startsWith(declared + ".")) {
            throw new IllegalArgumentException(
                    eventType + " est declare sur " + declared + ", publication tentee sur " + topic);
        }
    }

    @Override
    public void close() {
        if (delegate != null) {
            delegate.close();
        }
    }
}

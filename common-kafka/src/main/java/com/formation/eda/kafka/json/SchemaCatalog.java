package com.formation.eda.kafka.json;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Les schemas JSON du projet, charges depuis le classpath.
 *
 * <p>WHY: le <b>fichier de schema est le contrat</b>, pas la classe Java. Les serdes Confluent
 * savent deriver un schema d'une classe, et c'est tentant — mais le resultat n'est pas
 * pilotable : la derivation de {@code OrderPlaced} produit un titre {@code "Order Placed"}
 * (avec une espace), enveloppe chaque champ dans un {@code oneOf [null, …]} et ignore toute
 * contrainte metier (bornes, patterns, {@code minItems}). Autrement dit, le contrat publie
 * dans le Schema Registry dependrait des conventions de nommage d'une bibliotheque.
 *
 * <p>En ecrivant le schema a la main, on garde : le nom du sujet, les champs obligatoires, et
 * les contraintes que la classe Java ne sait pas exprimer. La classe devient une
 * implementation du contrat ; un service en Go ou en Python lirait le meme contrat.
 *
 * <p>Convention : {@code /schemas/<Titre>.json}, le titre etant aussi le {@code eventType} de
 * l'enveloppe. {@code /schemas/subjects.properties} dit sur quel topic chaque schema circule.
 */
public final class SchemaCatalog {

    private static final String SCHEMA_PATH = "schemas/%s.json";
    private static final String SUBJECTS_PATH = "schemas/subjects.properties";

    private final Map<String, JsonSchema> schemas = new ConcurrentHashMap<>();
    private final Map<String, String> topicsByEventType;

    public SchemaCatalog() {
        this.topicsByEventType = loadSubjects();
    }

    /**
     * Le schema du type d'evenement donne ({@code "OrderPlaced"}).
     *
     * <p>Echoue vite et fort : un evenement sans schema ne doit pas partir sur un topic. Le
     * silence serait pire — un message hors contrat que les consumers decouvriraient a
     * l'execution.
     */
    public JsonSchema schemaFor(String eventType) {
        return schemas.computeIfAbsent(eventType, type -> new JsonSchema(read(SCHEMA_PATH.formatted(type))));
    }

    /** Le sujet du Schema Registry, selon {@code TopicRecordNameStrategy} : topic + "-" + titre. */
    public String subjectFor(String eventType) {
        return topicFor(eventType) + "-" + eventType;
    }

    public String topicFor(String eventType) {
        String topic = topicsByEventType.get(eventType);
        if (topic == null) {
            throw new IllegalStateException(
                    "aucun topic declare pour " + eventType + " dans " + SUBJECTS_PATH);
        }
        return topic;
    }

    /** Tous les types d'evenements declares, dans l'ordre du fichier. */
    public Set<String> declaredEventTypes() {
        return topicsByEventType.keySet();
    }

    private static Map<String, String> loadSubjects() {
        Properties properties = new Properties();
        Resource resource = new ClassPathResource(SUBJECTS_PATH);
        try (InputStream in = resource.getInputStream()) {
            properties.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException("impossible de lire " + SUBJECTS_PATH, e);
        }
        Map<String, String> map = new LinkedHashMap<>();
        properties.stringPropertyNames().stream().sorted()
                .forEach(name -> map.put(name, properties.getProperty(name)));
        return Map.copyOf(map);
    }

    private static String read(String path) {
        try (InputStream in = new ClassPathResource(path).getInputStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("schema introuvable : " + path, e);
        }
    }
}

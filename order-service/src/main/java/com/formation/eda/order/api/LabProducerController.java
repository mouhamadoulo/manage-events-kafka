package com.formation.eda.order.api;

import com.formation.eda.events.EventHeaders;
import com.formation.eda.events.Topics;
import com.formation.eda.events.order.OrderLine;
import com.formation.eda.events.order.OrderPlaced;
import com.formation.eda.kafka.json.EventJsonSchemaSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Outil de rafale pour les experiences de la phase 3. <b>Profil {@code lab} uniquement.</b>
 *
 * <p>Les garanties d'un producteur ne se lisent pas, elles se mesurent : il faut pouvoir
 * envoyer quelques milliers de messages avec une configuration donnee, couper le broker au
 * milieu, et comparer ce que le producteur croit avoir envoye avec ce qui est reellement dans
 * le topic. Cet endpoint fabrique un producteur <b>jetable</b> par appel, avec les reglages
 * passes en parametres.
 *
 * <p>WHY un producteur par appel : {@code acks}, {@code enable.idempotence},
 * {@code linger.ms} et les autres sont figes a la construction du {@code KafkaProducer}. Les
 * comparer exige donc d'en construire plusieurs — ce qui est impensable en production, ou un
 * producteur est un objet lourd, partage et durable.
 *
 * <p>Ce controleur n'est PAS derriere le profil {@code lab} par precaution de style : il
 * permet d'ecrire en masse dans un topic metier et d'y injecter des doublons. Il n'a aucune
 * raison d'exister dans un service deploye.
 */
@RestController
@Profile("lab")
@RequestMapping("/lab/producer")
public class LabProducerController {

    private static final Logger log = LoggerFactory.getLogger(LabProducerController.class);

    private final String bootstrapServers;
    private final String schemaRegistryUrl;

    public LabProducerController(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.properties.schema.registry.url}") String schemaRegistryUrl) {
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    /** Ce que la rafale a produit, et ce que le producteur en sait. */
    public record BurstResult(
            int requested,
            int enqueued,
            int acknowledged,
            int failed,
            long durationMs,
            long messagesPerSecond,
            String acks,
            boolean idempotence,
            int lingerMs,
            int batchSize,
            String compression,
            int maxInFlight,
            boolean validate,
            int deliveryTimeoutMs,
            int maxBlockMs,
            String firstFailure) {
    }

    @PostMapping("/burst")
    public BurstResult burst(
            @RequestParam(defaultValue = "1000") int count,
            @RequestParam(defaultValue = "all") String acks,
            @RequestParam(defaultValue = "true") boolean idempotence,
            @RequestParam(defaultValue = "5") int lingerMs,
            @RequestParam(defaultValue = "32768") int batchSize,
            @RequestParam(defaultValue = "zstd") String compression,
            @RequestParam(defaultValue = "5") int maxInFlight,
            @RequestParam(defaultValue = "true") boolean validate,
            @RequestParam(defaultValue = "60000") int deliveryTimeoutMs,
            @RequestParam(defaultValue = "30000") int requestTimeoutMs,
            @RequestParam(defaultValue = "5000") int maxBlockMs,
            @RequestParam(defaultValue = "true") boolean abortOnFailure) {

        // Le producteur refuse de demarrer si delivery.timeout.ms < linger.ms +
        // request.timeout.ms :
        //   ConfigException: delivery.timeout.ms should be equal to or larger than
        //   linger.ms + request.timeout.ms
        // La contrainte est logique — le budget total doit couvrir une tentative complete —
        // et elle se declenche vite quand on bricole linger.ms pour mesurer le debit.
        int minimumDeliveryTimeout = lingerMs + requestTimeoutMs;
        if (deliveryTimeoutMs < minimumDeliveryTimeout) {
            deliveryTimeoutMs = minimumDeliveryTimeout;
        }

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EventJsonSchemaSerializer.class);
        config.put("schema.registry.url", schemaRegistryUrl);
        config.put(EventJsonSchemaSerializer.VALIDATE_PAYLOAD, validate);
        config.put(ProducerConfig.ACKS_CONFIG, acks);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotence);
        config.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlight);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        // max.block.ms borne le temps que send() passe A BLOQUER quand le buffer du
        // producteur est plein ou que les metadonnees manquent. Defaut 60 s : avec un broker
        // mort, le buffer (32 Mo) se remplit en quelques milliers de messages et chaque
        // send() suivant attend une minute. C'est ce qui fait croire a un "freeze" de
        // l'application alors que le producteur fait exactement ce qu'on lui a demande.
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
        if (!idempotence) {
            // Sans idempotence, retries doit etre explicite : c'est lui qui rend la
            // duplication possible (un lot reessaye que le broker avait deja ecrit).
            config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        }

        int enqueued = 0;
        AtomicInteger acknowledged = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        List<String> firstFailure = new ArrayList<>(1);

        ProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(config);
        long start = System.nanoTime();
        try {
            KafkaTemplate<String, Object> template = new KafkaTemplate<>(factory);
            List<CompletableFuture<SendResult<String, Object>>> futures = new ArrayList<>(count);

            for (int i = 0; i < count; i++) {
                enqueued = i + 1;
                OrderPlaced event = OrderPlaced.of(
                        "order-burst-" + i + "-" + System.nanoTime(),
                        "customer-" + (i % 50),
                        List.of(new OrderLine("SKU-" + (i % 20), 1 + (i % 3), new BigDecimal("7.90"))),
                        "EUR");
                // WHY ce try/catch : la serialisation se fait sur le thread appelant, AVANT
                // que le future existe. Quand le broker ou le registre tombe en pleine
                // rafale, send() leve directement — et sans ce filet, toute la rafale
                // s'arrete et l'endpoint renvoie 500 au lieu de rendre ses compteurs. Or
                // c'est precisement pendant une panne que les compteurs interessent.
                try {
                futures.add(template.send(Topics.ORDER_EVENTS, event.partitionKey(), event)
                        .whenComplete((result, failure) -> {
                            if (failure == null) {
                                acknowledged.incrementAndGet();
                            } else {
                                failed.incrementAndGet();
                                // Un seul message d'erreur retenu : en rafale, 10 000 lignes
                                // identiques n'apprennent rien de plus que la premiere.
                                synchronized (firstFailure) {
                                    if (firstFailure.isEmpty()) {
                                        firstFailure.add(failure.getClass().getSimpleName()
                                                + " : " + failure.getMessage());
                                    }
                                }
                            }
                        }));
                } catch (RuntimeException e) {
                    failed.incrementAndGet();
                    synchronized (firstFailure) {
                        if (firstFailure.isEmpty()) {
                            firstFailure.add("[synchrone] " + e.getClass().getSimpleName()
                                    + " : " + e.getMessage());
                        }
                    }
                    // Une fois le buffer plein, CHAQUE send() suivant bloque max.block.ms
                    // avant d'echouer : poursuivre la boucle sur 20 000 messages prend des
                    // heures pour n'apprendre rien de plus. S'arreter au premier echec est
                    // aussi ce qu'une application reelle fait — elle cesse d'accepter.
                    if (abortOnFailure) {
                        enqueued = i + 1;
                        break;
                    }
                }
            }

            // allOf echouerait des la premiere exception ; exceptionally(null) laisse chaque
            // future se terminer pour que les compteurs soient complets.
            CompletableFuture.allOf(futures.stream()
                    .map(f -> f.exceptionally(e -> null))
                    .toArray(CompletableFuture[]::new)).join();
        } finally {
            factory.reset();
            if (factory instanceof DefaultKafkaProducerFactory<String, Object> defaultFactory) {
                defaultFactory.destroy();
            }
        }

        long durationMs = Math.max(1, (System.nanoTime() - start) / 1_000_000);
        BurstResult result = new BurstResult(
                count, enqueued, acknowledged.get(), failed.get(), durationMs,
                count * 1000L / durationMs,
                acks, idempotence, lingerMs, batchSize, compression, maxInFlight,
                validate, deliveryTimeoutMs, maxBlockMs,
                firstFailure.isEmpty() ? null : firstFailure.get(0));

        log.info("rafale terminee : {}", result);
        return result;
    }

    /** Ce qu'une publication unitaire a produit. */
    public record SingleResult(
            String eventId,
            String orderId,
            String topic,
            int partition,
            long offset,
            Integer failTimes,
            Boolean failFatal) {
    }

    /**
     * Publie UN evenement, avec les headers d'injection de panne de la phase 4.
     *
     * <p>WHY cet endpoint existe : l'effet metier de notification-service est un compteur en
     * memoire, donc rien ne peut y echouer. Sans declencheur, les etages de retry, la branche
     * « erreur metier » de la DLT et le rejeu selectif n'auraient aucun moyen d'etre mesures.
     *
     * <p>Les headers ne sont lus par le consumer que sous profil {@code lab} : une panne ne
     * doit pas pouvoir etre commandee depuis un message en fonctionnement normal.
     *
     * @param failTimes nombre d'echecs transitoires a provoquer avant de reussir. 3 suffit a
     *                  traverser les deux etages et atteindre la DLT.
     * @param failFatal provoque un echec definitif : DLT immediate, sans etage de retry.
     */
    @PostMapping("/one")
    public SingleResult one(
            @RequestParam(required = false) Integer failTimes,
            @RequestParam(required = false) Boolean failFatal,
            @RequestParam(required = false) String orderId) {

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EventJsonSchemaSerializer.class);
        config.put("schema.registry.url", schemaRegistryUrl);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        OrderPlaced event = OrderPlaced.of(
                orderId == null ? "order-lab-" + System.nanoTime() : orderId,
                "customer-lab",
                List.of(new OrderLine("SKU-LAB", 1, new BigDecimal("4.20"))),
                "EUR");

        ProducerRecord<String, Object> record =
                new ProducerRecord<>(Topics.ORDER_EVENTS, null, event.partitionKey(), event);
        // Les memes trois headers que KafkaEventPublisher : sans eventId, l'intercepteur
        // d'idempotence du consumer traiterait le message SANS dedupliquer.
        header(record, EventHeaders.EVENT_ID, event.envelope().eventId().toString());
        header(record, EventHeaders.EVENT_TYPE, event.envelope().eventType());
        header(record, EventHeaders.CORRELATION_ID, event.envelope().correlationId());
        if (failTimes != null) {
            header(record, EventHeaders.FAIL_TIMES, String.valueOf(failTimes));
        }
        if (Boolean.TRUE.equals(failFatal)) {
            header(record, EventHeaders.FAIL_FATAL, "true");
        }

        ProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(config);
        try {
            RecordMetadata metadata = new KafkaTemplate<>(factory).send(record).join().getRecordMetadata();
            log.info("publication unitaire eventId={} failTimes={} failFatal={} -> {}-{} offset={}",
                    event.envelope().eventId(), failTimes, failFatal,
                    metadata.topic(), metadata.partition(), metadata.offset());
            return new SingleResult(
                    event.envelope().eventId().toString(), event.orderId(),
                    metadata.topic(), metadata.partition(), metadata.offset(),
                    failTimes, failFatal);
        } finally {
            factory.reset();
            if (factory instanceof DefaultKafkaProducerFactory<String, Object> defaultFactory) {
                defaultFactory.destroy();
            }
        }
    }

    private static void header(ProducerRecord<String, ?> record, String name, String value) {
        record.headers().add(name, value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}

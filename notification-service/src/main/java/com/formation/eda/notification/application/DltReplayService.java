package com.formation.eda.notification.application;

import com.formation.eda.events.EventHeaders;
import com.formation.eda.kafka.reliability.HeaderReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Inspection et rejeu selectif du dead letter topic.
 *
 * <p><b>Ce que le rejeu garantit, exactement.</b> Un message arrive en DLT apres un echec a vu
 * sa marque d'idempotence <b>effacee</b> (cf. {@code IdempotentRecordInterceptor}). Le rejeu le
 * <b>retraite</b> donc — c'est le but, sinon il ne servirait a rien. La propriete vraie est :
 * <b>rejouer deux fois ne produit qu'un effet</b>. Le premier rejeu traite et repose la marque,
 * le second est saute par le consumer. Ce service ne tient aucun historique de ses propres
 * appels : c'est {@code processed_event} qui arbitre, comme pour n'importe quelle autre
 * livraison.
 *
 * <p><b>Les octets sont republies tels quels.</b> Le consumer de lecture utilise un
 * {@link ByteArrayDeserializer}, et le message repart sur le topic metier sans etre decode.
 * Deux raisons : l'id de schema inscrit dans chaque message est <b>global</b> (verifie en
 * P2-T3 : les 4 sujets portent le meme id), donc des octets valides sur {@code .dlt} restent
 * valides sur le topic metier ; et un message dont la deserialisation avait echoue ne peut de
 * toute facon pas etre decode — le rejouer le renverra en DLT, ce qui est le comportement
 * correct.
 */
@Service
public class DltReplayService {

    /**
     * Borne dure. Un appel qui lirait 100 000 offsets tiendrait tout en memoire et bloquerait
     * le thread HTTP. Le rejeu est une operation d'administration ciblee, pas un outil de
     * migration.
     */
    public static final int MAX_OFFSETS_PER_CALL = 100;

    private static final Duration POLL = Duration.ofMillis(500);

    private static final Logger log = LoggerFactory.getLogger(DltReplayService.class);

    private final Map<String, Object> consumerConfigs;
    private final KafkaTemplate<String, byte[]> dltTemplate;

    public DltReplayService(ConsumerFactory<?, ?> consumerFactory,
                            @Qualifier("byteArrayDltTemplate") KafkaTemplate<String, byte[]> dltTemplate) {
        this.dltTemplate = dltTemplate;
        this.consumerConfigs = rawConsumerConfigs(consumerFactory);
    }

    /** Ce qu'une demande de rejeu designe. */
    public record ReplayRequest(String topic, int partition, List<Long> offsets) {
    }

    /** Ce qu'un rejeu a fait, offset par offset. */
    public record ReplayReport(int republished, List<Long> notFound, List<Long> rejected) {
    }

    /** Vue d'un message en DLT, telle qu'un operateur en a besoin pour decider. */
    public record DltRecordView(
            long offset,
            String key,
            String eventId,
            String originTopic,
            int retryStage,
            String exceptionType,
            String exceptionMessage,
            String failedAt) {
    }

    /**
     * Lit les derniers messages de {@code topic} sans rien consommer ni commiter.
     *
     * @param limit nombre maximum de messages rendus, borne a {@link #MAX_OFFSETS_PER_CALL}
     */
    public List<DltRecordView> peek(String topic, int limit) {
        int capped = Math.min(Math.max(limit, 1), MAX_OFFSETS_PER_CALL);
        List<DltRecordView> view = new ArrayList<>();

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerConfigs)) {
            for (TopicPartition partition : partitionsOf(consumer, topic)) {
                long end = consumer.endOffsets(List.of(partition)).get(partition);
                long begin = consumer.beginningOffsets(List.of(partition)).get(partition);
                if (end <= begin) {
                    continue;
                }
                // On remonte de `capped` messages depuis la fin : ce qui interesse un operateur
                // devant une DLT, ce sont les echecs RECENTS.
                consumer.assign(List.of(partition));
                consumer.seek(partition, Math.max(begin, end - capped));
                for (ConsumerRecord<String, byte[]> record : poll(consumer, end)) {
                    view.add(viewOf(record));
                }
            }
        }
        return view;
    }

    /**
     * Republie les offsets demandes sur leur topic d'origine.
     *
     * @throws IllegalArgumentException si plus de {@link #MAX_OFFSETS_PER_CALL} offsets sont
     *         demandes
     */
    public ReplayReport replay(ReplayRequest request) {
        if (request.offsets() == null || request.offsets().isEmpty()) {
            throw new IllegalArgumentException("au moins un offset est requis");
        }
        if (request.offsets().size() > MAX_OFFSETS_PER_CALL) {
            throw new IllegalArgumentException(
                    "au plus " + MAX_OFFSETS_PER_CALL + " offsets par appel, recu "
                            + request.offsets().size());
        }
        // WHY ce controle explicite : un offset nul dans la liste faisait exploser le TreeSet
        // plus bas sur « Cannot invoke Comparable.compareTo because k1 is null », donc un 500
        // pour une requete que le client a mal formee. Un client qui serialise une variable
        // vide envoie exactement cela.
        if (request.offsets().stream().anyMatch(java.util.Objects::isNull)) {
            throw new IllegalArgumentException("offset nul dans la liste demandee");
        }

        Set<Long> wanted = new TreeSet<>(request.offsets());
        List<Long> notFound = new ArrayList<>();
        List<Long> rejected = new ArrayList<>();
        int republished = 0;

        TopicPartition partition = new TopicPartition(request.topic(), request.partition());

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerConfigs)) {
            consumer.assign(List.of(partition));
            long end = consumer.endOffsets(List.of(partition)).get(partition);
            long begin = consumer.beginningOffsets(List.of(partition)).get(partition);

            // WHY lire les bornes AVANT de chercher : un offset au-dela de la fin du log
            // n'arrivera jamais, et poll() tournerait jusqu'au timeout du client HTTP.
            List<Long> reachable = new ArrayList<>();
            for (Long offset : wanted) {
                if (offset < begin || offset >= end) {
                    notFound.add(offset);
                } else {
                    reachable.add(offset);
                }
            }

            for (Long offset : reachable) {
                consumer.seek(partition, offset);
                ConsumerRecord<String, byte[]> record = first(poll(consumer, end), offset);
                if (record == null) {
                    notFound.add(offset);
                    continue;
                }
                if (republish(record)) {
                    republished++;
                } else {
                    rejected.add(offset);
                }
            }
        }

        ReplayReport report = new ReplayReport(republished, notFound, rejected);
        log.info("rejeu depuis {}-{} : {}", request.topic(), request.partition(), report);
        return report;
    }

    /** @return {@code false} si le record a ete refuse. */
    private boolean republish(ConsumerRecord<String, byte[]> record) {
        String originTopic = HeaderReader.string(record.headers(), EventHeaders.ORIGIN_TOPIC);
        if (originTopic == null || originTopic.isBlank()) {
            // WHY refuser : sans topic d'origine, la seule destination « evidente » serait le
            // topic lu — donc la DLT elle-meme. Ce serait une boucle infinie alimentee par
            // l'operateur, un message de plus a chaque appel.
            log.warn("rejeu refuse, pas de header {} : {}-{} offset={}",
                    EventHeaders.ORIGIN_TOPIC, record.topic(), record.partition(), record.offset());
            return false;
        }

        ProducerRecord<String, byte[]> republished =
                new ProducerRecord<>(originTopic, null, record.key(), record.value());

        // Seuls les headers de CONTRAT repartent. Les headers de diagnostic
        // (kafka_dlt-*, x-origin-*, x-retry-stage, x-failed-at) decrivent l'echec passe : les
        // recopier ferait croire au prochain echec que le message vient deja d'un etage de
        // retry, et x-origin-timestamp fausserait le calcul des echeances.
        copyHeader(record, republished, EventHeaders.EVENT_ID);
        copyHeader(record, republished, EventHeaders.EVENT_TYPE);
        copyHeader(record, republished, EventHeaders.CORRELATION_ID);

        dltTemplate.send(republished).join();
        return true;
    }

    private static void copyHeader(ConsumerRecord<String, byte[]> from,
                                   ProducerRecord<String, byte[]> to, String name) {
        Header header = from.headers().lastHeader(name);
        if (header != null) {
            to.headers().add(name, header.value());
        }
    }

    private DltRecordView viewOf(ConsumerRecord<String, byte[]> record) {
        return new DltRecordView(
                record.offset(),
                record.key(),
                HeaderReader.string(record.headers(), EventHeaders.EVENT_ID),
                HeaderReader.string(record.headers(), EventHeaders.ORIGIN_TOPIC),
                HeaderReader.intValue(record.headers(), EventHeaders.RETRY_STAGE, -1),
                HeaderReader.string(record.headers(), "kafka_dlt-exception-fqcn"),
                HeaderReader.string(record.headers(), "kafka_dlt-exception-message"),
                HeaderReader.string(record.headers(), EventHeaders.FAILED_AT));
    }

    /** Lit jusqu'a atteindre {@code end}, ou jusqu'a deux polls vides. */
    private List<ConsumerRecord<String, byte[]>> poll(KafkaConsumer<String, byte[]> consumer, long end) {
        List<ConsumerRecord<String, byte[]>> collected = new ArrayList<>();
        int emptyPolls = 0;
        while (emptyPolls < 2) {
            ConsumerRecords<String, byte[]> records = consumer.poll(POLL);
            if (records.isEmpty()) {
                emptyPolls++;
                continue;
            }
            records.forEach(collected::add);
            if (collected.stream().anyMatch(record -> record.offset() >= end - 1)) {
                break;
            }
        }
        return collected;
    }

    private static ConsumerRecord<String, byte[]> first(
            List<ConsumerRecord<String, byte[]>> records, long offset) {
        return records.stream()
                .filter(record -> record.offset() == offset)
                .findFirst()
                .orElse(null);
    }

    private static List<TopicPartition> partitionsOf(KafkaConsumer<String, byte[]> consumer, String topic) {
        return consumer.partitionsFor(topic).stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .toList();
    }

    /**
     * Configuration du consumer de lecture, derivee de celle du service.
     *
     * <p>Deux differences, et elles comptent toutes les deux :
     * <ul>
     *   <li><b>aucun {@code group.id}</b> — la lecture se fait par {@code assign} + {@code seek},
     *       donc elle ne commite aucun offset, ne declenche aucun rebalancing, et ne laisse
     *       aucune trace cote broker ;
     *   <li><b>deserialiseurs bruts</b> — les octets ne sont jamais decodes.
     * </ul>
     */
    private static Map<String, Object> rawConsumerConfigs(ConsumerFactory<?, ?> consumerFactory) {
        Map<String, Object> configs = new HashMap<>(consumerFactory.getConfigurationProperties());
        configs.remove(ConsumerConfig.GROUP_ID_CONFIG);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // Les proprietes du deserialiseur JSON Schema n'ont plus de sens ici et certaines
        // feraient echouer la construction du consumer brut.
        configs.keySet().removeIf(key -> key.startsWith("spring.deserializer") || key.startsWith("json."));
        return configs;
    }
}

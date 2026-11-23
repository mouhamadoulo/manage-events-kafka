package com.formation.eda.kafka.reliability;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerPausingBackOffHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerContainerPauseService;
import org.springframework.kafka.listener.ListenerContainerRegistry;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.invocation.MethodArgumentResolutionException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Clock;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Mecanique de fiabilite consumer : classement des erreurs, retry, DLT, idempotence.
 *
 * <p><b>WHY importee explicitement et non auto-configuree.</b> Il n'y a volontairement pas de
 * {@code META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports}
 * dans ce module. Un service qui veut cette mecanique ecrit
 * {@code @Import(ConsumerReliabilityConfiguration.class)}. Quatre raisons :
 * <ul>
 *   <li>les 6 services dependent de {@code common-kafka} ; une auto-configuration sans
 *       condition s'appliquerait aussi aux consumers d'{@code order-service} et au binder
 *       Kafka Streams d'{@code analytics-service}, ce qui n'est pas voulu ;
 *   <li>le profil {@code cloud-stream} de {@code notification-service} doit rester <b>nu</b>
 *       pour servir de temoin (P1-T7) : il ne peut l'etre que si rien ne s'active tout seul ;
 *   <li>une ligne {@code @Import} se lit ; une auto-configuration est une magie a expliquer,
 *       et ce depot existe pour expliquer ;
 *   <li>en phase 6, les 5 services de la saga prendront chacun ce qu'ils veulent.
 * </ul>
 *
 * <p>Le service doit fournir un bean {@link ProcessedEventStore}. WHY ne pas le fournir ici
 * sous condition : {@code @ConditionalOnBean(JdbcClient.class)} dans une configuration
 * utilisateur est evalue AVANT les auto-configurations, donc la condition serait toujours
 * fausse et le bean jamais cree. Et sans condition, un service sans base ne pourrait plus
 * importer cette configuration. Le registre depend de la persistance du service : c'est au
 * service de le declarer.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ConsumerReliabilityProperties.class)
public class ConsumerReliabilityConfiguration {

    /**
     * Backoff des reessais <b>bloquants</b>, sur le topic principal.
     *
     * <p>Volontairement court. Ce retry occupe le thread du consumer : pendant qu'il attend,
     * les evenements suivants de la meme partition ne sont pas traites. Il est la pour
     * l'incident d'une demi-seconde — une reconnexion, un verrou momentane — pas pour attendre
     * un service en panne. Au-dela, c'est le role des topics de retry, qui liberent la
     * partition.
     *
     * <p>WHY {@link ExponentialBackOff} de spring-core et non
     * {@code org.springframework.kafka.support.ExponentialBackOffWithMaxRetries} : les deux
     * existent sur cette stack (verifie par {@code javap}), mais seul le premier a gagne
     * {@code setJitter}. Sans jitter, N instances du meme groupe reessaient a la meme
     * milliseconde et frappent ensemble la dependance qui vient de tomber.
     */
    public static ExponentialBackOff blockingBackOff(ConsumerReliabilityProperties props) {
        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(props.blockingInitialInterval().toMillis());
        backOff.setMultiplier(props.blockingMultiplier());
        // maxAttempts compte les REESSAIS : a 2, le listener est appele 3 fois en tout.
        backOff.setMaxAttempts(props.blockingRetries());
        backOff.setJitter(props.blockingJitter().toMillis());
        return backOff;
    }

    /**
     * Gestionnaire d'erreur du topic principal : quelques reessais bloquants, puis passage au
     * recoverer, qui publie vers l'etage de retry suivant (tache 5).
     *
     * <p>Expose en {@code static} pour etre testable sans contexte Spring : le classement des
     * exceptions est la decision la plus consequente de cette phase, elle merite des tests
     * directs.
     */
    public static DefaultErrorHandler blockingErrorHandler(ConsumerRecordRecoverer recoverer,
                                                           ConsumerReliabilityProperties props) {
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, blockingBackOff(props));

        // WHY: un message qui ne se decode pas ne se decodera pas mieux dans 400 ms. Le
        // reessayer coute le temps d'attente ET retarde le signalement du vrai probleme.
        //
        // Les quatre premieres sont DEJA non-retryables par defaut dans
        // ExceptionClassifier de spring-kafka (verifie par javap sur le jar : la carte
        // initiale contient DeserializationException, MessageConversionException,
        // MethodArgumentResolutionException, ConversionException, ClassCastException et
        // NoSuchMethodException). On les redeclare quand meme : ce depot existe pour que le
        // classement se LISE dans le code, et un defaut de framework qui change en silence a
        // la version suivante ne doit pas changer notre comportement.
        FATAL_EXCEPTIONS.forEach(handler::addNotRetryableExceptions);

        // WHY explicite alors que tout est retryable par defaut : l'intention doit se lire
        // dans le code. Un lecteur ne doit pas avoir a connaitre le defaut du framework pour
        // savoir ce qui est reessaye.
        handler.addRetryableExceptions(TransientProcessingException.class);

        // WHY: sans cela, l'offset d'un message publie en DLT n'est pas commite — il serait
        // repolle au prochain cycle, et republie en DLT indefiniment.
        handler.setCommitRecovered(true);

        return handler;
    }

    @Bean
    DefaultErrorHandler mainErrorHandler(ConsumerRecordRecoverer recoverer,
                                         ConsumerReliabilityProperties props) {
        return blockingErrorHandler(recoverer, props);
    }

    /**
     * Exceptions qui envoient un message directement en dead letter topic, sans passer par les
     * etages de retry.
     *
     * <p>Une seule liste, utilisee aux deux endroits qui doivent s'accorder : le classement du
     * gestionnaire d'erreur, et le resolveur de destination. Deux listes separees pourraient
     * divergér, et un message classe non-retryable mais route vers un etage de retry y
     * resterait a tourner jusqu'a la DLT — 35 secondes pour rien.
     */
    static final List<Class<? extends Exception>> FATAL_EXCEPTIONS = List.of(
            DeserializationException.class,
            MethodArgumentResolutionException.class,
            MessageConversionException.class,
            ConversionException.class,
            ClassCastException.class,
            NonRetryableEventException.class);

    /**
     * @return {@code true} si {@code exception}, ou n'importe laquelle de ses causes, est un
     *         echec definitif.
     *
     * <p>WHY parcourir les causes : le conteneur emballe l'exception du listener dans une
     * {@code ListenerExecutionFailedException}, et un echec de deserialisation arrive
     * lui-meme emballe. Tester le type de surface classerait tout comme retryable.
     */
    static boolean isFatal(Throwable exception) {
        for (Throwable current = exception; current != null; current = cause(current)) {
            for (Class<? extends Exception> fatal : FATAL_EXCEPTIONS) {
                if (fatal.isInstance(current)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Ou part un message qui vient d'echouer : l'etage de retry suivant, ou directement la DLT
     * si l'echec est definitif.
     *
     * <p>La partition est laissee a {@code -1} : le recoverer n'impose alors aucune partition et
     * le partitionneur la choisit depuis la cle. L'ordre par agregat est donc conserve a
     * l'interieur d'un etage. Imposer la partition d'origine casserait de toute facon la
     * publication sur un {@code .dlt} qui n'a qu'une partition.
     */
    static BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver(
            ConsumerReliabilityProperties props) {
        return (record, exception) -> {
            String topic = record.topic();
            // Rien ne consomme la DLT, mais un routage qui la renverrait vers un etage
            // creerait une boucle : .dlt -> .retry-1 -> .retry-2 -> .dlt -> ...
            if (isFatal(exception) || RetryTopology.stageOf(topic) < 0) {
                return new TopicPartition(RetryTopology.deadLetterOf(topic), -1);
            }
            return new TopicPartition(RetryTopology.next(topic, props.stageCount()), -1);
        };
    }

    private static Throwable cause(Throwable exception) {
        return exception.getCause() == exception ? null : exception.getCause();
    }

    /**
     * Horloge injectable : les tests du differe ont besoin d'une {@link Clock#fixed} pour
     * asserter une duree restante exacte plutot qu'un intervalle approximatif.
     */
    @Bean
    @ConditionalOnMissingBean(Clock.class)
    Clock systemClock() {
        return Clock.systemUTC();
    }

    @Bean
    DiagnosticHeaders diagnosticHeaders(Clock clock) {
        return new DiagnosticHeaders(clock);
    }

    /**
     * Template de publication des payloads <b>bruts</b>.
     *
     * <p>WHY un second template : un echec de deserialisation n'a pas d'objet a republier,
     * seulement les octets d'origine, que le recoverer recupere dans la
     * {@code DeserializationException}. Les passer au serialiseur JSON Schema echouerait une
     * seconde fois — et cette fois a l'interieur du mecanisme qui devait sauver le message.
     *
     * <p>La configuration est copiee de celle du producteur du service (meme broker, memes
     * reglages de fiabilite), en remplacant les seuls serialiseurs.
     */
    /**
     * Template de publication des payloads <b>typés</b>, avec le serialiseur du service.
     *
     * <p>WHY declarer ce template alors que Boot en fournit un : le bean {@code kafkaTemplate}
     * de {@code KafkaAutoConfiguration} est {@code @ConditionalOnMissingBean(KafkaTemplate.class)}.
     * Des que ce module declare {@code byteArrayDltTemplate}, celui de Boot <b>n'est plus
     * cree</b>. Le recoverer recevait alors le meme template pour les deux types de payload et
     * echouait sur
     * {@code SerializationException: Can't convert value of class OrderPlaced to class
     * ByteArraySerializer} — bug trouve par {@code RetryChainTest}, invisible en test unitaire.
     */
    @Bean
    KafkaTemplate<String, Object> eventDltTemplate(ProducerFactory<?, ?> producerFactory) {
        return new KafkaTemplate<>(typedProducer(producerFactory));
    }

    @SuppressWarnings("unchecked")
    private static ProducerFactory<String, Object> typedProducer(ProducerFactory<?, ?> producerFactory) {
        return (ProducerFactory<String, Object>) producerFactory;
    }

    @Bean
    KafkaTemplate<String, byte[]> byteArrayDltTemplate(ProducerFactory<?, ?> producerFactory) {
        Map<String, Object> configs = new HashMap<>(producerFactory.getConfigurationProperties());
        configs.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        configs.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(configs));
    }

    /**
     * Publie un message en echec vers l'etage de retry suivant, ou vers la DLT.
     *
     * <p>Le template est choisi par <b>type de payload</b> : les octets bruts d'un echec de
     * deserialisation passent par le serialiseur ByteArray, tout le reste par le template du
     * service (JSON Schema, dont le sujet {@code ....dlt-OrderPlaced} est deja enregistre
     * depuis P2-T3).
     *
     * <p>{@code setAppendOriginalHeaders(false)} : sans cela, les headers
     * {@code kafka_dlt-original-*} s'empilent a chaque etage et un message en DLT en porte
     * trois jeux. Nos propres {@code x-origin-*}, eux, sont poses une seule fois par
     * {@link DiagnosticHeaders}.
     */
    @Bean
    DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            @Qualifier("eventDltTemplate") KafkaTemplate<String, Object> eventDltTemplate,
            @Qualifier("byteArrayDltTemplate") KafkaTemplate<String, byte[]> byteArrayDltTemplate,
            ConsumerReliabilityProperties props,
            DiagnosticHeaders diagnosticHeaders) {

        // LinkedHashMap : le recoverer parcourt les entrees dans l'ordre et prend la premiere
        // dont la classe accepte le payload. Object.class accepterait aussi un byte[].
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, byteArrayDltTemplate);
        templates.put(Object.class, eventDltTemplate);

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(templates, destinationResolver(props));
        recoverer.setHeadersFunction(diagnosticHeaders);
        recoverer.setAppendOriginalHeaders(false);
        // WHY: sans cela, une publication en DLT qui echoue est journalisee et oubliee. Le
        // message disparait alors que tout le mecanisme existait pour le garder.
        recoverer.setFailIfSendResultIsError(true);
        return recoverer;
    }

    @Bean
    RetryStageSupport retryStageSupport(ConsumerReliabilityProperties props, Clock clock) {
        return new RetryStageSupport(props, clock);
    }

    /**
     * Ordonnanceur dedie a la reprise des partitions mises en pause.
     *
     * <p>WHY un bean a nous et non le {@code TaskScheduler} de Boot : Boot n'en configure un que
     * si l'application active l'ordonnancement ({@code @EnableScheduling}). Un service qui
     * importe cette configuration sans ordonnancement n'aurait aucun {@code TaskScheduler}, et
     * le contexte echouerait au demarrage sur une dependance manquante — pour un mecanisme qui
     * n'a besoin que de poser un reveil.
     *
     * <p>Un seul thread suffit : il ne fait qu'appeler {@code resume} a l'echeance.
     */
    @Bean
    ThreadPoolTaskScheduler retryPauseScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("retry-pause-");
        // Sans cela, l'arret de l'application n'attend pas les reprises en vol et une partition
        // peut rester en pause jusqu'au prochain demarrage.
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(5);
        return scheduler;
    }

    @Bean
    ListenerContainerPauseService listenerContainerPauseService(
            ListenerContainerRegistry registry,
            ThreadPoolTaskScheduler retryPauseScheduler) {
        return new ListenerContainerPauseService(registry, retryPauseScheduler);
    }

    /**
     * Gestionnaire d'erreur des conteneurs d'etage de retry.
     *
     * <p>Deux comportements dans un seul objet, distingues par le type d'exception :
     *
     * <ol>
     *   <li>{@link NotYetDueException} — le record est arrive trop tot. Le backoff vaut
     *       exactement la duree restante, et le {@link PartitionPausingBackOffHandler} met
     *       sa partition en pause au lieu de faire dormir le thread. L'offset n'est pas
     *       commite : le record est redelivre a la reprise, il est alors du, et il passe.
     *   <li><b>tout le reste</b> — un echec reel dans un etage de retry. Zero reessai
     *       bloquant : le record part immediatement vers l'etage suivant, ou vers la DLT.
     *       WHY zero : le delai de l'etage <i>est</i> l'attente. Y ajouter un backoff
     *       bloquant ferait attendre deux fois, dont une en occupant le thread.
     * </ol>
     *
     * <p>WHY {@code setBackOffFunction} et non un backoff fixe : la fonction recoit le record
     * <b>et l'exception</b>. C'est la seule facon d'attendre la duree restante propre a chaque
     * record — sans elle, il faudrait un pas fixe et re-verifier l'echeance a chaque reveil.
     */
    @Bean
    DefaultErrorHandler retryErrorHandler(ConsumerRecordRecoverer recoverer,
                                          ListenerContainerPauseService pauseService) {
        BackOff straightToNextStage = new FixedBackOff(0L, 0L);
        DefaultErrorHandler handler = new DefaultErrorHandler(
                recoverer, straightToNextStage, new PartitionPausingBackOffHandler(pauseService));

        handler.setBackOffFunction((record, exception) ->
                rootNotYetDue(exception) instanceof NotYetDueException notYetDue
                        // 3 tentatives et non 1 : une horloge qui derive de quelques
                        // millisecondes entre le producteur et le consumer ferait sinon partir
                        // en DLT un message dont le seul tort est d'etre arrive trop tot.
                        ? new FixedBackOff(notYetDue.remainingMillis(), 3L)
                        : straightToNextStage);

        // Retryable, impérativement : non-retryable, un record arrive trop tot partirait en DLT
        // au premier passage.
        handler.addRetryableExceptions(NotYetDueException.class);
        FATAL_EXCEPTIONS.forEach(handler::addNotRetryableExceptions);
        handler.setCommitRecovered(true);

        // Explicite plutot qu'herite du defaut : le compteur de tentatives d'un record est
        // remis a zero quand l'exception change. C'est ce qui empeche une attente d'echeance de
        // consommer les tentatives d'un echec reel survenu juste apres.
        handler.setResetStateOnExceptionChange(true);

        return handler;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, Object> retryContainerFactory(
            ConsumerFactory<?, ?> consumerFactory,
            @Qualifier("retryErrorHandler") DefaultErrorHandler retryErrorHandler,
            IdempotentRecordInterceptor idempotentRecordInterceptor) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(typed(consumerFactory));
        factory.setCommonErrorHandler(retryErrorHandler);
        factory.setRecordInterceptor(idempotentRecordInterceptor);
        return factory;
    }

    /**
     * Le conteneur enveloppe l'exception du listener dans une
     * {@code ListenerExecutionFailedException}. Sans ce deballage, le differe ne serait jamais
     * reconnu et chaque record non echu partirait a l'etage suivant immediatement.
     */
    static Throwable rootNotYetDue(Throwable exception) {
        Throwable current = exception;
        while (current != null && !(current instanceof NotYetDueException)) {
            current = current.getCause() == current ? null : current.getCause();
        }
        return current;
    }

    @Bean
    IdempotentRecordInterceptor idempotentRecordInterceptor(ProcessedEventStore store) {
        return new IdempotentRecordInterceptor(store);
    }

    /**
     * Fabrique de conteneurs du topic principal.
     *
     * <p>L'ordre compte : l'intercepteur d'idempotence tourne <b>avant</b> le listener, et le
     * gestionnaire d'erreur <b>apres</b> son echec.
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, Object> mainContainerFactory(
            ConsumerFactory<?, ?> consumerFactory,
            DefaultErrorHandler mainErrorHandler,
            IdempotentRecordInterceptor idempotentRecordInterceptor) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(typed(consumerFactory));
        factory.setCommonErrorHandler(mainErrorHandler);
        factory.setRecordInterceptor(idempotentRecordInterceptor);
        return factory;
    }

    /**
     * Le bean de Boot est un {@code DefaultKafkaConsumerFactory<?, ?>} : ses types sont decides
     * par la configuration ({@code key-deserializer} / {@code value-deserializer}), pas par le
     * code. La conversion est donc sans risque reel ici, et la faire une seule fois dans une
     * methode nommee vaut mieux que de la repandre.
     */
    @SuppressWarnings("unchecked")
    static ConsumerFactory<String, Object> typed(ConsumerFactory<?, ?> consumerFactory) {
        return (ConsumerFactory<String, Object>) consumerFactory;
    }

}

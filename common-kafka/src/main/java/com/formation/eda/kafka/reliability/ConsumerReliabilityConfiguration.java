package com.formation.eda.kafka.reliability;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.invocation.MethodArgumentResolutionException;
import org.springframework.util.backoff.ExponentialBackOff;

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
        handler.addNotRetryableExceptions(
                DeserializationException.class,
                MethodArgumentResolutionException.class,
                MessageConversionException.class,
                ConversionException.class,
                ClassCastException.class,
                NonRetryableEventException.class);

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

    /**
     * Implementation Postgres du registre d'idempotence, fournie seulement si le service a
     * effectivement du JDBC.
     *
     * <p>WHY cette condition : {@code spring-boot-starter-jdbc} est {@code optional} dans ce
     * module (cf. pom), donc {@link JdbcClient} peut etre absent du classpath du service.
     * {@code @ConditionalOnClass} sur une classe imbriquee evite que la configuration
     * englobante soit illisible dans ce cas. {@code @ConditionalOnMissingBean} laisse un
     * service fournir son propre store — ce que fera un test, ou un service sans base.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(JdbcClient.class)
    static class JdbcProcessedEventStoreConfiguration {

        @Bean
        @ConditionalOnMissingBean(ProcessedEventStore.class)
        ProcessedEventStore processedEventStore(JdbcClient jdbcClient) {
            return new JdbcProcessedEventStore(jdbcClient);
        }
    }
}

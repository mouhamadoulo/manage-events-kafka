package com.formation.eda.notification.infrastructure.persistence;

import com.formation.eda.kafka.reliability.JdbcProcessedEventStore;
import com.formation.eda.kafka.reliability.ProcessedEventStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Registre d'idempotence, branche sur la base {@code notifications}.
 *
 * <p>WHY ce bean est declare ici et non dans {@code common-kafka} : le registre depend de la
 * persistance du service. {@code spring-boot-starter-jdbc} y est {@code optional} — les 5
 * autres services n'ont pas de base — donc {@code common-kafka} ne peut pas supposer qu'un
 * {@link JdbcClient} existe. Une condition {@code @ConditionalOnBean(JdbcClient.class)} ne
 * reglerait rien : dans une configuration utilisateur, elle est evaluee AVANT les
 * auto-configurations, donc toujours fausse.
 */
@Configuration(proxyBeanMethods = false)
public class PersistenceConfiguration {

    @Bean
    ProcessedEventStore processedEventStore(JdbcClient jdbcClient) {
        return new JdbcProcessedEventStore(jdbcClient);
    }
}

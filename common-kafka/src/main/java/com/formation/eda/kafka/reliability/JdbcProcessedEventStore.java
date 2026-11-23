package com.formation.eda.kafka.reliability;

import org.springframework.jdbc.core.simple.JdbcClient;

import java.util.UUID;

/**
 * Implementation Postgres de {@link ProcessedEventStore}.
 *
 * <p>WHY {@link JdbcClient} et non JPA : l'operation est un {@code INSERT ... ON CONFLICT DO
 * NOTHING} dont on lit le <b>nombre de lignes affectees</b>. JPA n'exprime pas cela — un
 * {@code persist()} suivi d'un {@code flush()} dans un {@code try/catch} de
 * {@code DataIntegrityViolationException} donnerait le meme resultat en masquant le mecanisme,
 * et en salissant la session Hibernate au passage. Ici, la requete SQL <b>est</b> le pattern.
 *
 * <p>Le resultat ne depend d'aucune lecture prealable : c'est la contrainte d'unicite de la
 * base qui arbitre, donc deux instances concurrentes du meme groupe ne peuvent pas traiter le
 * meme evenement.
 */
public class JdbcProcessedEventStore implements ProcessedEventStore {

    private static final String INSERT = """
            insert into processed_event (event_id, consumer_group, source_topic)
            values (?, ?, ?)
            on conflict (event_id, consumer_group) do nothing
            """;

    private static final String DELETE = """
            delete from processed_event
            where event_id = ? and consumer_group = ?
            """;

    private final JdbcClient jdbcClient;

    public JdbcProcessedEventStore(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Override
    public boolean markProcessed(UUID eventId, String consumerGroup, String sourceTopic) {
        // 1 ligne inseree = premier traitement. 0 ligne = la cle existait, donc doublon.
        return jdbcClient.sql(INSERT)
                .param(eventId)
                .param(consumerGroup)
                .param(sourceTopic)
                .update() == 1;
    }

    @Override
    public void forget(UUID eventId, String consumerGroup) {
        jdbcClient.sql(DELETE)
                .param(eventId)
                .param(consumerGroup)
                .update();
    }
}

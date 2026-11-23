package com.formation.eda.notification.infrastructure.persistence;

import com.formation.eda.kafka.reliability.JdbcProcessedEventStore;
import com.formation.eda.kafka.reliability.ProcessedEventStore;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * La table {@code processed_event} contre un vrai Postgres.
 *
 * <p>WHY un test d'integration et pas un test unitaire : tout le mecanisme repose sur le
 * <b>nombre de lignes affectees</b> par un {@code INSERT ... ON CONFLICT DO NOTHING}. C'est la
 * contrainte d'unicite de la base qui arbitre, et elle seule : un faux store en memoire
 * prouverait seulement que le code appelle le store.
 *
 * <p>Classe nommee {@code *IT} : lancee par failsafe sous {@code -Pintegration} uniquement
 * (Docker requis). Le profil {@code integration} generalise est la phase 10 (P10-T3).
 *
 * <p>Testcontainers <b>2.0.5</b> : artefact {@code testcontainers-postgresql} et classe
 * {@code org.testcontainers.postgresql.PostgreSQLContainer}, non generique. Les exemples 1.x
 * utilisent {@code org.testcontainers.containers.PostgreSQLContainer<SELF>} et l'artefact
 * {@code postgresql} — ni l'un ni l'autre ne s'appliquent ici.
 */
class JdbcProcessedEventStoreIT {

    private static final String GROUP = "notification-service";
    private static final String OTHER_GROUP = "analytics-service";
    private static final String TOPIC = "shop.order.events.v1";

    // Meme image que l'infra locale (infra/docker-compose.yml) : tester contre une autre
    // version majeure de Postgres apprendrait des choses sur la mauvaise base.
    @SuppressWarnings("resource")
    private static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer("postgres:18-alpine")
                    .withDatabaseName("notifications")
                    .withUsername("eda")
                    .withPassword("eda");

    private static JdbcClient jdbcClient;
    private ProcessedEventStore store;

    @BeforeAll
    static void startDatabase() {
        POSTGRES.start();

        DataSource dataSource = dataSource();
        // La MEME migration que celle embarquee dans le jar de common-kafka : si le DDL etait
        // recopie ici, le test validerait une table que la production n'a pas.
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration/shared")
                .load()
                .migrate();

        jdbcClient = JdbcClient.create(dataSource);
    }

    @AfterAll
    static void stopDatabase() {
        POSTGRES.stop();
    }

    @BeforeEach
    void setUp() {
        jdbcClient.sql("truncate table processed_event").update();
        store = new JdbcProcessedEventStore(jdbcClient);
    }

    @Test
    @DisplayName("la migration partagee est appliquee depuis le jar de common-kafka")
    void theSharedMigrationIsApplied() {
        List<String> applied = jdbcClient.sql("select script from flyway_schema_history order by installed_rank")
                .query(String.class)
                .list();

        assertThat(applied).contains("V1__processed_event.sql");
    }

    @Test
    @DisplayName("le premier insert affecte une ligne, le second zero")
    void theFirstInsertAffectsOneRowTheSecondZero() {
        UUID eventId = UUID.randomUUID();

        assertThat(store.markProcessed(eventId, GROUP, TOPIC)).isTrue();
        assertThat(store.markProcessed(eventId, GROUP, TOPIC)).isFalse();
    }

    @Test
    @DisplayName("le meme eventId dans deux groupes affecte une ligne chacun")
    void theSameEventIdInTwoGroupsAffectsOneRowEach() {
        UUID eventId = UUID.randomUUID();

        assertThat(store.markProcessed(eventId, GROUP, TOPIC)).isTrue();
        // La raison d'etre de la cle composite : en phase 6, les 5 services de la saga lisent
        // les memes topics dans 5 groupes, et chacun doit traiter l'evenement une fois.
        assertThat(store.markProcessed(eventId, OTHER_GROUP, TOPIC)).isTrue();

        assertThat(jdbcClient.sql("select count(*) from processed_event where event_id = ?")
                .param(eventId).query(Long.class).single()).isEqualTo(2L);
    }

    @Test
    @DisplayName("le topic source ne fait pas partie de l'identite de l'evenement")
    void theSourceTopicIsNotPartOfTheEventIdentity() {
        UUID eventId = UUID.randomUUID();

        assertThat(store.markProcessed(eventId, GROUP, TOPIC)).isTrue();
        // Un record venu d'un etage de retry est le MEME evenement. Si source_topic entrait
        // dans la cle, chaque etage rejouerait l'effet metier.
        assertThat(store.markProcessed(eventId, GROUP, TOPIC + ".retry-1")).isFalse();
    }

    @Test
    @DisplayName("forget permet un nouvel insert")
    void forgetAllowsANewInsert() {
        UUID eventId = UUID.randomUUID();

        store.markProcessed(eventId, GROUP, TOPIC);
        store.forget(eventId, GROUP);

        // C'est ce qui rend le retry possible : l'echec defait la marque.
        assertThat(store.markProcessed(eventId, GROUP, TOPIC)).isTrue();
    }

    @Test
    @DisplayName("forget ne touche que le groupe demande")
    void forgetOnlyTouchesTheRequestedGroup() {
        UUID eventId = UUID.randomUUID();
        store.markProcessed(eventId, GROUP, TOPIC);
        store.markProcessed(eventId, OTHER_GROUP, TOPIC);

        store.forget(eventId, GROUP);

        assertThat(store.markProcessed(eventId, OTHER_GROUP, TOPIC)).isFalse();
        assertThat(store.markProcessed(eventId, GROUP, TOPIC)).isTrue();
    }

    @Test
    @DisplayName("processed_at est renseigne par la base, pas par l'application")
    void processedAtIsSetByTheDatabase() {
        UUID eventId = UUID.randomUUID();
        store.markProcessed(eventId, GROUP, TOPIC);

        // WHY: l'horloge d'une instance de consumer peut deriver. Celle de la base est unique
        // pour tous les consumers, donc la seule comparable entre eux.
        assertThat(jdbcClient.sql("select processed_at from processed_event where event_id = ?")
                .param(eventId).query(java.time.OffsetDateTime.class).single()).isNotNull();
    }

    /**
     * WHY {@link DriverManagerDataSource} et non {@code PGSimpleDataSource} : le pilote
     * Postgres est en portee {@code runtime}, donc absent du classpath de COMPILATION des
     * tests. Celui-ci charge le pilote par son nom a l'execution.
     */
    private static DataSource dataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setUrl(POSTGRES.getJdbcUrl());
        dataSource.setUsername(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        return dataSource;
    }
}

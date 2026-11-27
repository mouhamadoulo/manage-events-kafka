# Phase 4 — Fiabilité consumer : plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `notification-service` survit à un message empoisonné et à un rejeu : retry étagé, DLT exploitable, idempotence par groupe de consumers.

**Architecture:** Les briques réutilisables vivent dans `common-kafka` (store d'idempotence, intercepteur, classement d'erreurs, résolveur de destination, recoverer, fabriques de conteneurs), rassemblées dans une `@Configuration` que chaque service **importe explicitement** — jamais d'auto-configuration. `notification-service` porte un seul handler métier appelé par trois listeners (topic principal, `.retry-1`, `.retry-2`). Le différé d'un étage passe par une exception et la mise en pause de la partition, jamais par un thread qui dort.

**Tech Stack:** Java 25, Spring Boot 4.2.0, spring-kafka 4.2.0, kafka-clients 4.4.0, Confluent JSON Schema 8.4.1, Postgres 18, Flyway, Testcontainers 2.0.5.

**Spec:** [docs/superpowers/specs/2026-11-23-phase-4-fiabilite-consumer-design.md](../specs/2026-11-23-phase-4-fiabilite-consumer-design.md)

## Global Constraints

- `JAVA_HOME` sur `C:\Users\mouha\Documents\WorkspaceJAVA\1-MesJDKs\MicrosoftOpenJDKs\jdk-25.0.2+10` avant tout build. Le JDK du `PATH` est un 21 et échoue sur `release version 25 not supported`.
- Noms de topics figés par l'infra : `shop.<x>.events.v1`, `.retry-1`, `.retry-2`, `.dlt`. Aucun renommage.
- `auto.create.topics.enable=false` côté broker, `auto.register.schemas=false` côté client : aucun topic ni sujet créé par effet de bord.
- `spring-boot-starter-jdbc` et `flyway-core` dans `common-kafka` sont **`<optional>true</optional>`**. Les 6 services dépendent de `common-kafka` et 5 n'ont pas de base.
- Les beans de `common-kafka` arrivent par `@Import`, **jamais** par `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`.
- `processed_event` : `PRIMARY KEY (event_id, consumer_group)`. `source_topic` hors de la clé.
- Jackson 3 (`tools.jackson.databind`), mais annotations en `com.fasterxml.jackson.annotation`.
- Pas de Lombok sur les records. Un package par couche : `domain`, `application`, `infrastructure.kafka`, `infrastructure.persistence`, `api`.
- Commentaires `// WHY:` sur le choix, pas sur le fonctionnement. Toute propriété Kafka non évidente d'un `application.yml` est commentée avec son effet et son défaut.
- Un commit par tâche, Conventional Commits, scope = module.
- Zéro `Thread.sleep` dans les tests : Awaitility (déjà au classpath de tous les modules).

## Review Focus

Classes d'entrée que la spec implique sans qu'aucune tâche ne les teste d'elle-même. Chaque ligne a son test ajouté à la tâche qui porte le code.

1. **Record de `.dlt` sans header `x-origin-topic`** → le rejeu doit refuser explicitement. Sans ce garde-fou il republie sur `.dlt` lui-même : boucle infinie alimentée par l'opérateur. *(Tâche 8)*
2. **Offset de rejeu au-delà du log end offset** → réponse propre listant les offsets introuvables, pas un `poll()` qui tourne jusqu'au timeout du client HTTP. *(Tâche 8)*
3. **Header `eventId` présent mais non parsable en UUID** → l'intercepteur ne doit pas lever : une exception dans `intercept()` remonte au conteneur avant tout classement d'erreur. `WARN` + passage sans déduplication. *(Tâche 2)*
4. **Record d'un topic de retry sans `x-origin-timestamp`** (publié à la main par un opérateur) → échéance incalculable : traiter immédiatement, sinon le record est mis en pause indéfiniment. *(Tâche 4)*
5. **Tombstone (`value == null`) sur un topic métier** → pas de déduplication (l'`eventId` n'est pas fiable), et le handler n'est pas appelé. Le garde-fou existe déjà dans `OrderEventsListener` et doit survivre au refactor. *(Tâche 2 et tâche 6)*

---

## Structure des fichiers

**`common-events`** — contrat, aucune dépendance Kafka.
- Create `src/main/java/com/formation/eda/events/EventHeaders.java` — noms des headers, constantes seules.

**`common-kafka`** — briques réutilisables. Nouveau package `com.formation.eda.kafka.reliability`.
- `ProcessedEventStore.java` — interface, pas de JDBC.
- `JdbcProcessedEventStore.java` — `JdbcClient`.
- `IdempotentRecordInterceptor.java` — `RecordInterceptor`.
- `RetryTopology.java` — nommage et étages, statique et pur.
- `NotYetDueException.java`, `TransientProcessingException.java`, `NonRetryableEventException.java`.
- `RecordHeaders.java` — lecture typée d'un header (`String`, `long`, `int`).
- `ConsumerReliabilityProperties.java` — `@ConfigurationProperties("eda.reliability")`.
- `ConsumerReliabilityConfiguration.java` — la seule `@Configuration`, importée par les services.
- `src/main/resources/db/migration/shared/V1__processed_event.sql`.

**`notification-service`** — branchement et code métier.
- `application/OrderNotificationHandler.java` — l'effet métier, appelé par les 3 listeners.
- `application/FaultInjection.java` — profil `lab` seulement.
- `infrastructure/kafka/OrderEventsListener.java` *(modifié)* — délègue au handler.
- `infrastructure/kafka/OrderRetryListener.java` — les 2 listeners d'étage.
- `api/DltAdminController.java` — inspection et rejeu.
- `application/DltReplayService.java` — consumer sans groupe, republication.

**`order-service`** — `LabProducerController.java` *(modifié)* : headers d'injection de panne.

**`infra/experiments/p4-t6-poison.ps1`** — expérience message empoisonné.

---

## Task 1: `EventHeaders` dans `common-events`

**Files:**
- Create: `common-events/src/main/java/com/formation/eda/events/EventHeaders.java`
- Modify: `order-service/src/main/java/com/formation/eda/order/infrastructure/kafka/KafkaEventPublisher.java:38-40`
- Test: `common-events/src/test/java/com/formation/eda/events/EventHeadersTest.java`

**Interfaces:**
- Produces: `EventHeaders.EVENT_ID` = `"eventId"`, `EVENT_TYPE` = `"eventType"`, `CORRELATION_ID` = `"correlationId"`, `ORIGIN_TOPIC` = `"x-origin-topic"`, `ORIGIN_TIMESTAMP` = `"x-origin-timestamp"`, `RETRY_STAGE` = `"x-retry-stage"`, `FAILED_AT` = `"x-failed-at"`, `FAIL_TIMES` = `"x-fail-times"`, `FAIL_FATAL` = `"x-fail-fatal"`. Classe `final`, constructeur privé.

**Note :** `common-events` n'a **aucune** dépendance `kafka-clients`. Donc constantes seules ici : la lecture d'un `org.apache.kafka.common.header.Headers` est dans `RecordHeaders` de `common-kafka` (tâche 2).

- [ ] **Step 1: Write the failing test**

```java
@Test
void les_noms_de_headers_sont_ceux_du_contrat() {
    assertThat(EventHeaders.EVENT_ID).isEqualTo("eventId");
    assertThat(EventHeaders.ORIGIN_TOPIC).isEqualTo("x-origin-topic");
    assertThat(EventHeaders.ORIGIN_TIMESTAMP).isEqualTo("x-origin-timestamp");
    assertThat(EventHeaders.RETRY_STAGE).isEqualTo("x-retry-stage");
}

@Test
void le_publisher_utilise_les_constantes_partagees() {
    assertThat(KafkaEventPublisher.HEADER_EVENT_ID).isEqualTo(EventHeaders.EVENT_ID);
}
```
Le second test vit dans `order-service/src/test/java/.../KafkaEventPublisherHeadersTest.java` (`common-events` ne voit pas `order-service`).

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -pl common-events test -Dtest=EventHeadersTest`
Expected: FAIL, `EventHeaders` n'existe pas.

- [ ] **Step 3: Create `EventHeaders`, puis faire pointer les 3 constantes de `KafkaEventPublisher` dessus**

Garder les constantes `HEADER_*` de `KafkaEventPublisher` comme alias (`= EventHeaders.EVENT_ID`) : elles sont `public` et déjà utilisées. Le javadoc « un header se lit sans désérialiser » déménage dans `EventHeaders`.

- [ ] **Step 4: Run tests**

Run: `./mvnw -pl common-events -am test` puis `./mvnw -pl order-service -am test`
Expected: PASS, et les 11 tests existants de `common-events` toujours verts.

- [ ] **Step 5: Commit**

```bash
git add common-events/src order-service/src
git commit -m "refactor(common-events): noms de headers d'evenement partages"
```

---

## Task 2: idempotence — store, migration, intercepteur *(P4-T1, P4-T2)*

**Files:**
- Create: `common-kafka/src/main/java/com/formation/eda/kafka/reliability/{ProcessedEventStore,JdbcProcessedEventStore,IdempotentRecordInterceptor,RecordHeaders}.java`
- Create: `common-kafka/src/main/resources/db/migration/shared/V1__processed_event.sql`
- Modify: `common-kafka/pom.xml` (ajout `spring-boot-starter-jdbc` et `flyway-core`, **tous deux `<optional>true</optional>`**)
- Test: `common-kafka/src/test/java/com/formation/eda/kafka/reliability/IdempotentRecordInterceptorTest.java`

**Interfaces:**
- Consumes: `EventHeaders.EVENT_ID` (tâche 1).
- Produces:
  - `interface ProcessedEventStore { boolean markProcessed(UUID eventId, String consumerGroup, String sourceTopic); void forget(UUID eventId, String consumerGroup); }`
  - `JdbcProcessedEventStore(JdbcClient jdbcClient)`
  - `IdempotentRecordInterceptor(ProcessedEventStore store) implements RecordInterceptor<String, Object>`
  - `RecordHeaders.string(Headers headers, String name)` → `String` ou `null` ; `RecordHeaders.longValue(Headers, String)` → `OptionalLong` ; `RecordHeaders.intValue(Headers, String, int defaultValue)` → `int`.

**Contrat de `markProcessed` :** `true` = ligne insérée, le record doit être traité. `false` = la ligne existait, c'est un doublon. Pas de `SELECT` préalable : `SELECT` puis `INSERT` est une condition de course entre deux instances du même groupe, la contrainte d'unicité est le seul arbitre.

- [ ] **Step 1: Write the failing tests** — `IdempotentRecordInterceptorTest`, avec un `ProcessedEventStore` en mémoire (`Set<String>` de `eventId|group`) et un `Consumer` mocké dont `groupMetadata().groupId()` rend `"notification-service"`.

```java
@Test
void un_premier_passage_laisse_le_record_passer()            // intercept() != null
@Test
void un_second_passage_du_meme_eventId_est_saute()           // intercept() == null
@Test
void un_eventId_identique_dans_un_autre_groupe_passe()       // deux groupes, deux fois non nul
@Test
void un_record_venu_de_retry_1_est_vu_comme_le_meme_evenement()
        // meme eventId, sourceTopic different -> second appel nul
@Test
void un_record_sans_header_eventId_passe_sans_deduplication()
        // intercept() != null, et le store n'a rien enregistre
@Test
void un_eventId_non_parsable_passe_sans_lever()              // header "pas-un-uuid"
@Test
void un_record_a_valeur_nulle_passe_sans_deduplication()     // tombstone
@Test
void failure_retire_la_marque()
        // intercept(), puis failure(), puis intercept() -> non nul
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -pl common-kafka -am test -Dtest=IdempotentRecordInterceptorTest`
Expected: FAIL, classes absentes.

- [ ] **Step 3: Implement `RecordHeaders`, `ProcessedEventStore`, `IdempotentRecordInterceptor`**

`intercept()` : lire le header, `UUID.fromString` dans un `try` (un `IllegalArgumentException` donne un `WARN` et un passage), sauter si `record.value() == null`, sinon `markProcessed(eventId, consumer.groupMetadata().groupId(), record.topic())` et rendre `null` sur `false`.
Le javadoc de la classe doit porter le piège de la spec §7.2 : `forget()` ne part que si le handler **lève**, donc tout chemin qui sort sans exception après interception perd le record.

- [ ] **Step 4: Run tests**

Run: `./mvnw -pl common-kafka -am test -Dtest=IdempotentRecordInterceptorTest`
Expected: PASS, 8 tests.

- [ ] **Step 5: Écrire la migration et l'implémentation JDBC**

`V1__processed_event.sql` : le DDL de la spec §7.2, verbatim, avec un commentaire SQL sur la clé composite.
`JdbcProcessedEventStore.markProcessed` : `insert into processed_event (event_id, consumer_group, source_topic) values (?,?,?) on conflict do nothing`, retourne `update() == 1`.

- [ ] **Step 6: Run the whole build**

Run: `./mvnw clean verify`
Expected: BUILD SUCCESS, 9 modules, **les 6 tests de contexte toujours verts** — c'est ce qui prouve que les deux dépendances `optional` ne sont pas transitives.

- [ ] **Step 7: Commit**

```bash
git add common-kafka
git commit -m "feat(common-kafka): idempotence consumer par table processed_event (P4-T1, P4-T2)"
```

---

## Task 3: classement des erreurs et `DefaultErrorHandler` *(P4-T3)*

**Files:**
- Create: `common-kafka/src/main/java/com/formation/eda/kafka/reliability/{NonRetryableEventException,TransientProcessingException}.java`
- Create: `common-kafka/src/main/java/com/formation/eda/kafka/reliability/ConsumerReliabilityProperties.java`
- Create: `common-kafka/src/main/java/com/formation/eda/kafka/reliability/ConsumerReliabilityConfiguration.java`
- Test: `common-kafka/src/test/java/com/formation/eda/kafka/reliability/ErrorClassificationTest.java`

**Interfaces:**
- Produces:
  - `record ConsumerReliabilityProperties(List<Duration> stageDelays, int blockingRetries, Duration blockingInitialInterval, double blockingMultiplier, Duration blockingJitter)`, préfixe `eda.reliability`, défauts `5s,30s` / `2` / `200ms` / `2.0` / `50ms`.
  - `ConsumerReliabilityConfiguration` expose `DefaultErrorHandler mainErrorHandler(ConsumerRecordRecoverer recoverer, ConsumerReliabilityProperties props)` et `ConcurrentKafkaListenerContainerFactory<String, Object> mainContainerFactory(ConsumerFactory<String, Object> cf, DefaultErrorHandler mainErrorHandler, IdempotentRecordInterceptor interceptor)`. Le recoverer réel arrive en tâche 5 ; ici le bean prend un `ConsumerRecordRecoverer` injecté, et le test fournit un enregistreur.
  - La classe n'est importée par aucun service avant la tâche 6 : aucun contexte ne l'instancie, donc une dépendance manquante ne casse pas le build de cette tâche.
  - `NonRetryableEventException(String message)` et `TransientProcessingException(String message, Throwable cause)`, les deux `extends RuntimeException`.

- [ ] **Step 1: Write the failing test** — `ErrorClassificationTest`, en appelant directement `handler.handleRemaining(...)` / en inspectant `ExceptionClassifier`, sans broker.

```java
@Test
void une_exception_non_retryable_part_directement_au_recoverer()
        // DeserializationException -> recoverer appele au 1er echec
@Test
void une_exception_transitoire_est_reessayee_puis_recuperee()
        // TransientProcessingException -> 3 tentatives (1 + 2 retries) puis recoverer
@Test
void NonRetryableEventException_est_classee_non_retryable()
@Test
void le_backoff_porte_du_jitter()   // ExponentialBackOff.getJitter() == 50ms
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -pl common-kafka test -Dtest=ErrorClassificationTest`
Expected: FAIL.

- [ ] **Step 3: Implement le bean `mainErrorHandler`**

`ExponentialBackOff` de **spring-core 7.1.0** (`setInitialInterval`, `setMultiplier`, `setMaxAttempts`, `setJitter`) — pas `ExponentialBackOffWithMaxRetries`, retenu pour le jitter.
`addNotRetryableExceptions(DeserializationException.class, MethodArgumentResolutionException.class, MessageConversionException.class, ClassCastException.class, NonRetryableEventException.class)`, `addRetryableExceptions(TransientProcessingException.class)`, `setCommitRecovered(true)`.
Chaque appel porte un `// WHY:` d'une ligne.

- [ ] **Step 4: Run test**

Run: `./mvnw -pl common-kafka test -Dtest=ErrorClassificationTest`
Expected: PASS, 4 tests.

- [ ] **Step 5: Commit**

```bash
git add common-kafka
git commit -m "feat(common-kafka): classement retryable/non-retryable et backoff bloquant (P4-T3)"
```

---

## Task 4: étages de retry et différé par mise en pause *(P4-T4)*

**Files:**
- Create: `common-kafka/src/main/java/com/formation/eda/kafka/reliability/{RetryTopology,NotYetDueException,RetryStageSupport}.java`
- Modify: `common-kafka/.../ConsumerReliabilityConfiguration.java`
- Test: `common-kafka/src/test/java/com/formation/eda/kafka/reliability/{RetryTopologyTest,RetryStageSupportTest}.java`

**Interfaces:**
- Consumes: `ConsumerReliabilityProperties.stageDelays()` (tâche 3), `EventHeaders.ORIGIN_TIMESTAMP` (tâche 1), `RecordHeaders.longValue` (tâche 2).
- Produces:
  - `RetryTopology.next(String topic)` → topic suivant ; `IllegalArgumentException` si `topic` finit déjà par `.dlt`.
  - `RetryTopology.stageOf(String topic)` → `0` principal, `1`, `2`, `-1` pour `.dlt`.
  - `RetryTopology.baseTopicOf(String topic)` → topic métier sans suffixe.
  - `NotYetDueException(long remainingMillis)` avec `long remainingMillis()`.
  - `RetryStageSupport(ConsumerReliabilityProperties props, Clock clock)` et
    `void requireDue(ConsumerRecord<?, ?> record)` → lève `NotYetDueException` si l'échéance n'est pas atteinte, ne fait rien sinon. Exposé en bean par `ConsumerReliabilityConfiguration`, avec un bean `Clock systemClock()` (`Clock.systemUTC()`) pour que les tests puissent le remplacer.
  - Beans `DefaultErrorHandler retryErrorHandler(...)`, `ListenerContainerPauseService listenerContainerPauseService(ListenerContainerRegistry registry, TaskScheduler scheduler)` et `ConcurrentKafkaListenerContainerFactory<String, Object> retryContainerFactory(ConsumerFactory<String, Object> cf, DefaultErrorHandler retryErrorHandler, IdempotentRecordInterceptor interceptor)` — c'est le nom que la tâche 6 passe en `containerFactory = "retryContainerFactory"`.

- [ ] **Step 1: Write the failing tests**

```java
// RetryTopologyTest
@Test void le_topic_principal_mene_au_premier_etage()   // shop.order.events.v1 -> ....retry-1
@Test void le_premier_etage_mene_au_second()
@Test void le_second_etage_mene_a_la_dlt()
@Test void la_dlt_n_a_pas_de_suivant()                  // IllegalArgumentException
@Test void stageOf_lit_l_etage_depuis_le_nom()          // 0 / 1 / 2 / -1
@Test void baseTopicOf_retire_le_suffixe()              // les 4 formes -> shop.order.events.v1

// RetryStageSupportTest, avec Clock.fixed
@Test void un_record_echu_passe()                        // origin-timestamp = T-6s, etage 1 -> rien
@Test void un_record_non_echu_leve_avec_la_duree_restante()
        // origin-timestamp = T-1s, etage 1 -> NotYetDueException, remainingMillis ~ 4000
@Test void un_record_sans_origin_timestamp_est_traite_immediatement()   // aucun throw
@Test void un_etage_inconnu_est_refuse()                 // IllegalArgumentException
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -pl common-kafka test -Dtest='RetryTopologyTest,RetryStageSupportTest'`
Expected: FAIL.

- [ ] **Step 3: Implement `RetryTopology`, `NotYetDueException`, `RetryStageSupport`**

`RetryStageSupport` lit `x-origin-timestamp` ; absent → retour immédiat (point 4 du Review Focus), avec un `WARN` nommant le topic et l'offset.

- [ ] **Step 4: Run tests**

Run: `./mvnw -pl common-kafka test -Dtest='RetryTopologyTest,RetryStageSupportTest'`
Expected: PASS, 10 tests.

- [ ] **Step 5: Implement `retryErrorHandler` et `listenerContainerPauseService`**

```java
DefaultErrorHandler h = new DefaultErrorHandler(
        recoverer, defaultBackOff, new ContainerPausingBackOffHandler(pauseService));
h.setBackOffFunction((record, ex) -> ex instanceof NotYetDueException n
        ? new FixedBackOff(n.remainingMillis(), 3)
        : defaultBackOff);
```
C'est l'unique algorithme du plan qu'une signature ne détermine pas : `setBackOffFunction` est ce qui permet un délai **par record** au lieu d'un pas fixe. `ContainerPausingBackOffHandler` met en pause la seule partition concernée, et l'offset n'est **pas** commité : le record est redélivré à la reprise. `NotYetDueException` doit être **retryable** (sinon elle part en DLT au premier passage).
`// WHY:` obligatoire sur les trois points : pourquoi une exception plutôt qu'un retour silencieux (sans elle, `RecordInterceptor.failure()` ne part pas et la marque d'idempotence reste), pourquoi une pause plutôt qu'un `sleep` (`max.poll.interval.ms`), pourquoi par partition plutôt que par conteneur.

- [ ] **Step 6: Commit**

```bash
git add common-kafka
git commit -m "feat(common-kafka): etages de retry et differe par mise en pause de partition (P4-T4)"
```

---

## Task 5: `DeadLetterPublishingRecoverer` et headers de diagnostic *(P4-T5)*

**Files:**
- Modify: `common-kafka/.../ConsumerReliabilityConfiguration.java`
- Create: `common-kafka/src/main/java/com/formation/eda/kafka/reliability/DiagnosticHeaders.java`
- Test: `common-kafka/src/test/java/com/formation/eda/kafka/reliability/DiagnosticHeadersTest.java`

**Interfaces:**
- Consumes: `RetryTopology.next/stageOf` (tâche 4), `EventHeaders.*` (tâche 1).
- Produces:
  - `DiagnosticHeaders(Clock clock) implements BiFunction<ConsumerRecord<?,?>, Exception, Headers>` — la `setHeadersFunction` du recoverer. Bean, avec le `Clock` de la tache 4.
  - Beans `KafkaTemplate<String, Object> jsonSchemaDltTemplate`, `KafkaTemplate<String, byte[]> byteArrayDltTemplate`, `DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(...)`.

- [ ] **Step 1: Write the failing test**

```java
@Test void x_origin_topic_est_pose_une_seule_fois()
        // record de .retry-1 portant deja x-origin-topic=shop.order.events.v1
        // -> une seule valeur, inchangee
@Test void x_origin_topic_est_pose_depuis_le_topic_principal()
@Test void x_origin_timestamp_est_pose_une_seule_fois()
@Test void x_retry_stage_suit_l_etage_du_record()        // 0, puis 1, puis 2
@Test void x_failed_at_porte_l_instant_de_mise_en_dlt()  // Clock.fixed
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -pl common-kafka test -Dtest=DiagnosticHeadersTest`
Expected: FAIL.

- [ ] **Step 3: Implement `DiagnosticHeaders`**

Poser `x-origin-topic` et `x-origin-timestamp` **seulement si absents** : les headers `kafka_dlt-original-*` du framework sont réécrits à chaque étage, le rejeu et le calcul d'échéance ont besoin du point de départ.

- [ ] **Step 4: Run test**

Run: `./mvnw -pl common-kafka test -Dtest=DiagnosticHeadersTest`
Expected: PASS, 5 tests.

- [ ] **Step 5: Implement le recoverer et les deux templates**

Constructeur `DeadLetterPublishingRecoverer(Map<Class<?>, KafkaOperations<?, ?>> templates, BiFunction<ConsumerRecord<?,?>, Exception, TopicPartition> destinationResolver)`.
Map : `byte[].class` → template `ByteArraySerializer`, `Object.class` → template JSON Schema.
Résolveur : `new TopicPartition(RetryTopology.next(record.topic()), -1)` — partition négative = laissée au partitionneur, la clé est conservée. Pour une exception non-retryable, viser directement `RetryTopology.baseTopicOf(topic) + ".dlt"`.
`setHeadersFunction(diagnosticHeaders)`, `setAppendOriginalHeaders(false)`.
`// WHY:` sur le choix de deux templates : un échec de désérialisation n'a pas d'objet à republier, seulement des octets ; les passer au sérialiseur JSON Schema échouerait une seconde fois.

- [ ] **Step 6: Run the whole build**

Run: `./mvnw clean verify`
Expected: BUILD SUCCESS, 9 modules.

- [ ] **Step 7: Commit**

```bash
git add common-kafka
git commit -m "feat(common-kafka): DLT avec headers de cause et payload brut preserve (P4-T5)"
```

---

## Task 6: branchement de `notification-service`

**Files:**
- Create: `notification-service/src/main/java/com/formation/eda/notification/application/OrderNotificationHandler.java`
- Create: `notification-service/src/main/java/com/formation/eda/notification/application/FaultInjection.java`
- Create: `notification-service/src/main/java/com/formation/eda/notification/infrastructure/kafka/OrderRetryListener.java`
- Modify: `notification-service/src/main/java/com/formation/eda/notification/infrastructure/kafka/OrderEventsListener.java`
- Modify: `notification-service/src/main/java/com/formation/eda/notification/NotificationServiceApplication.java` (`@Import`)
- Modify: `notification-service/src/main/resources/application.yml`, `notification-service/pom.xml`
- Modify: `order-service/src/main/java/com/formation/eda/order/api/LabProducerController.java`
- Test: `notification-service/src/test/java/com/formation/eda/notification/infrastructure/kafka/RetryChainTest.java` (`@EmbeddedKafka`)
- Test: `notification-service/src/test/java/com/formation/eda/notification/application/OrderNotificationHandlerTest.java`
- Test: `notification-service/src/test/java/com/formation/eda/notification/infrastructure/persistence/JdbcProcessedEventStoreIT.java` (Testcontainers)

**Interfaces:**
- Consumes: tout `com.formation.eda.kafka.reliability` (tâches 2 à 5).
- Produces: `OrderNotificationHandler.handle(ConsumerRecord<String, OrderPlaced> record)` — appelé par les 3 listeners. `FaultInjection.check(Headers headers, UUID eventId)` → lève `TransientProcessingException` ou `NonRetryableEventException`, ou ne fait rien.

**Décision de structure (spec §7.5) :** un handler, trois listeners, **dans le service**. Pas de listener génerique dans `common-kafka` : on ne connaîtra la bonne forme qu'en phase 6, avec 5 services × 2 étages. À revoir en P6-T1.

- [ ] **Step 1: Write the failing tests**

`OrderNotificationHandlerTest` (unitaire, pas de Kafka) :
```java
@Test void un_evenement_valide_incremente_le_compteur()
@Test void un_record_a_valeur_nulle_est_ignore_sans_lever()      // tombstone, cf. Review Focus 5
@Test void x_fail_times_2_leve_TransientProcessingException_deux_fois_puis_reussit()
@Test void x_fail_fatal_leve_NonRetryableEventException()
@Test void les_headers_d_injection_sont_ignores_hors_profil_lab()
```

`RetryChainTest` avec `@EmbeddedKafka(topics = {...principal, .retry-1, .retry-2, .dlt})`, payloads `String` et sérialiseurs `String` — la mécanique de retry est indifférente au payload, ce qui évite d'avoir besoin du Schema Registry dans ce test. Awaitility, pas de `sleep` :
```java
@Test void un_echec_transitoire_traverse_les_deux_etages_puis_atteint_la_dlt()
        // delais ramenes a 300ms/600ms par @DynamicPropertySource -> test en ~2s
        // asserts : 1 record dans .retry-1, 1 dans .retry-2, 1 dans .dlt
@Test void un_record_pas_encore_du_est_traite_apres_la_pause_une_seule_fois()
        // LE test du piege : handler appele exactement 1 fois, jamais 0
@Test void un_echec_non_retryable_va_directement_en_dlt()        // .retry-1 reste vide
@Test void deux_livraisons_du_meme_eventId_ne_produisent_qu_un_effet()   // P4-T2 de bout en bout
```

`JdbcProcessedEventStoreIT` (`postgres:18-alpine` via Testcontainers 2.0.5 — **API v2**, ne pas copier d'exemple 1.x) :
```java
@Test void le_premier_insert_affecte_une_ligne_le_second_zero()
@Test void le_meme_eventId_dans_deux_groupes_affecte_une_ligne_chacun()
@Test void forget_permet_un_nouvel_insert()
@Test void la_migration_partagee_est_appliquee()   // flyway_schema_history contient V1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -pl notification-service -am test`
Expected: FAIL.

- [ ] **Step 3: Implement le handler, l'injection de panne et les trois listeners**

`OrderEventsListener` garde son garde-fou tombstone et son log, et délègue l'effet à `OrderNotificationHandler`. `OrderRetryListener` porte deux méthodes `@KafkaListener(topics = "...retry-1" | "...retry-2", containerFactory = "retryContainerFactory")`, chacune appelant `retryStageSupport.requireDue(record)` puis le même handler.
`FaultInjection` est `@Profile("lab")` ; hors profil, un bean no-op. Une panne ne doit pas pouvoir être commandée depuis un message en fonctionnement normal.

- [ ] **Step 4: Implement la configuration**

`pom.xml` : `spring-boot-starter-jdbc`, `flyway-core`, `flyway-database-postgresql` (runtime), `org.postgresql:postgresql` (runtime), Testcontainers `postgresql` (test).
`application.yml` : `spring.datasource.*` (défauts `localhost:5432/notifications`, `eda`/`eda`, surchargeables par variables d'env), `spring.flyway.locations: classpath:db/migration/shared,classpath:db/migration`, le passage de `value-deserializer` à `ErrorHandlingDeserializer` avec `spring.deserializer.value.delegate.class`, une section `producer` (sérialiseur JSON Schema, nécessaire pour publier en DLT), et `eda.reliability.stage-delays: 5s,30s`. **Chaque propriété non évidente commentée avec son effet et son défaut**, dont celle qui remplace le commentaire « faille volontaire de la phase 1 » devenu obsolète.
`NotificationServiceApplication` : `@Import(ConsumerReliabilityConfiguration.class)`.

- [ ] **Step 5: Implement les headers d'injection côté `LabProducerController`**

Paramètres de requête optionnels `failTimes` et `failFatal`, posés en headers `x-fail-times` / `x-fail-fatal` sur le record. Profil `lab` déjà en place sur ce contrôleur.

- [ ] **Step 6: Run tests**

Run: `./mvnw -pl notification-service -am test`
puis `./mvnw -pl notification-service -am verify -Pintegration` (Docker requis)
Expected: PASS. Coller le compte de tests dans `docs/TASKS.md`.

- [ ] **Step 7: Run the whole build**

Run: `./mvnw clean verify`
Expected: BUILD SUCCESS, 9 modules, 6 contextes verts.

- [ ] **Step 8: Commit**

```bash
git add notification-service order-service
git commit -m "feat(notification-service): consumer fiable branche, handler unique et trois listeners"
```

---

## Task 7: expérience message empoisonné *(P4-T6)*

**Files:**
- Create: `infra/experiments/p4-t6-poison.ps1`

**Interfaces:**
- Consumes: l'application lancée (`./mvnw -pl notification-service spring-boot:run`), l'infra Docker up.

**Modèle à suivre :** `infra/experiments/p3-t3-perte.ps1` — même en-tête de documentation, même style de mesure, mêmes garde-fous PowerShell 5.1 déjà documentés dans CLAUDE.md §5 (`$ErrorActionPreference = "Continue"` autour des outils Kafka qui écrivent sur stderr ; passer par un fichier + `docker cp` plutôt qu'un pipe, à cause du BOM UTF-8).

- [ ] **Step 1: Écrire le script**

Déroulé : 10 `POST /orders` sur la même clé → 1 message brut sans octet magique injecté sur cette partition par `kafka-console-producer` → 10 `POST /orders` de plus → mesures.
Mesures à afficher : lag par partition (`kafka-consumer-groups.sh --describe`) avant et après, nombre de records dans `.dlt`, `GET /stats`.

- [ ] **Step 2: Exécuter le script, infra up et service lancé**

Run: `./infra/experiments/p4-t6-poison.ps1`
Expected: **1 message dans `.dlt`, 20 traités, lag revenu à 0.**

- [ ] **Step 3: Exécuter le témoin**

Relancer le service avec `ErrorHandlingDeserializer` désactivé (variable d'env introduite en tâche 6), rejouer le script.
Expected: lag **qui ne redescend pas** sur la partition empoisonnée, et la preuve que les 10 messages suivants ne sont jamais consommés. C'est la démonstration que la phase 1 avait laissée en place exprès.

- [ ] **Step 4: Commit**

```bash
git add infra/experiments/p4-t6-poison.ps1
git commit -m "feat(infra): experience message empoisonne, avec et sans ErrorHandlingDeserializer (P4-T6)"
```

---

## Task 8: endpoint d'inspection et de rejeu de la DLT *(P4-T7)*

**Files:**
- Create: `notification-service/src/main/java/com/formation/eda/notification/application/DltReplayService.java`
- Create: `notification-service/src/main/java/com/formation/eda/notification/api/DltAdminController.java`
- Test: `notification-service/src/test/java/com/formation/eda/notification/application/DltReplayServiceTest.java` (`@EmbeddedKafka`)

**Interfaces:**
- Consumes: `EventHeaders.ORIGIN_TOPIC` (tâche 1), `ProcessedEventStore` (tâche 2).
- Produces:
  - `record ReplayRequest(String topic, int partition, List<Long> offsets)`
  - `record ReplayReport(int republished, List<Long> notFound, List<Long> rejected)`
  - `DltReplayService.replay(ReplayRequest request)` → `ReplayReport`
  - `DltReplayService.peek(String topic, int limit)` → `List<DltRecordView>` avec
    `record DltRecordView(long offset, String key, String originTopic, String exceptionType, Instant failedAt)`
  - `POST /admin/dlt/replay`, `GET /admin/dlt`

**Garde-fous (Review Focus 1 et 2) :** un record sans `x-origin-topic` est **rejeté**, jamais republié (sinon il repart sur `.dlt` : boucle infinie). Un offset au-delà du log end offset est rendu dans `notFound`, sans `poll()` bloquant : lire `endOffsets()` **avant** de chercher. Borne dure à **100** offsets par appel.

- [ ] **Step 1: Write the failing test**

```java
@Test void un_record_de_dlt_est_republie_sur_son_topic_d_origine()
        // cle conservee, headers eventId/eventType/correlationId recopies,
        // headers de diagnostic absents du record republie
@Test void un_record_sans_x_origin_topic_est_rejete()           // rejected, rien de publie
@Test void un_offset_au_dela_de_la_fin_du_log_est_signale()      // notFound, pas de blocage
@Test void plus_de_100_offsets_est_refuse()                      // IllegalArgumentException
@Test void le_rejeu_ne_commite_aucun_offset_de_groupe()
        // assign/seek sans groupe : --describe ne montre pas de nouveau groupe
@Test void deux_rejeux_du_meme_offset_ne_produisent_qu_un_effet()
        // LA propriete de T7 : le 1er retraite et repose la marque, le 2e est saute
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -pl notification-service test -Dtest=DltReplayServiceTest`
Expected: FAIL.

- [ ] **Step 3: Implement `DltReplayService`**

`KafkaConsumer` construit à la main avec `ByteArrayDeserializer` clé et valeur, **sans `group.id`**, `assign` + `seek`. Les octets sont republiés **tels quels** : les ids de schéma sont globaux (vérifié en P2-T3, les 4 sujets portent `id=4`), donc un message valide sur `.dlt` reste valide sur le topic principal, et on évite une seconde désérialisation ratée. Un message empoisonné rejoué repartira en DLT — comportement correct, à documenter.

- [ ] **Step 4: Implement `DltAdminController`**

Javadoc obligatoire : console d'administration **sans authentification**, comme le reste du labo ; n'écoute que sur `127.0.0.1` ; borne de 100 ; P12-T8 la passera derrière un jeton.

- [ ] **Step 5: Run tests**

Run: `./mvnw -pl notification-service -am test`
Expected: PASS.

- [ ] **Step 6: Vérifier en vrai**

Sur l'infra up, avec le message en DLT produit par la tâche 7 : `GET /admin/dlt`, puis deux `POST /admin/dlt/replay` du même offset. Noter le compteur `/stats` après chaque appel.
Expected: premier rejeu → compteur +1 ; second → **+0**.

- [ ] **Step 7: Commit**

```bash
git add notification-service
git commit -m "feat(notification-service): inspection et rejeu selectif de la DLT (P4-T7)"
```

---

## Task 9: fiche pattern et suivi *(P4-T8)*

**Files:**
- Create: `docs/patterns/04-retry-dlq-idempotence.md`
- Modify: `docs/TASKS.md` (8 cases cochées, tableau de bord, journal)
- Modify: `CLAUDE.md` (§4 : clé composite ; §5 : pièges trouvés en route)

**Interfaces:** aucune. Toutes les mesures viennent des tâches 2 à 8.

**Modèle à suivre :** `docs/patterns/03-garanties-producer.md` — le problème, le mécanisme Kafka utilisé, le code clé, **comment le casser pour voir l'échec**, comment le vérifier.

- [ ] **Step 1: Écrire la fiche**

Doit contenir, avec les mesures réelles collectées :
- le piège « marquer avant de traiter », les trois réponses possibles, et pourquoi le différé a d'abord cassé l'idempotence dans ce projet même (spec §11, point 1) ;
- pourquoi une pause de partition et non un `sleep` : `max.poll.interval.ms`, et le rebalancing en boucle ;
- pourquoi `ListenerContainerIdleEvent` ne convient pas pour reprendre une partition (jamais publié sous trafic continu) ;
- la comparaison avec `@RetryableTopic` : ce qu'il câble (`ContainerPartitionPausingBackOffManager`, `KafkaBackoffAwareMessageListenerAdapter`, `DeadLetterPublishingRecovererFactory`) et le décalage de nommage mesuré par `javap` (`joinWithRetrySuffix` colle un `-` en dur, index à partir de 0) ;
- ce que le rejeu garantit exactement : **pas** « un rejeu est sauté », mais « deux rejeux, un seul effet » ;
- l'ordre partiel perdu entre topic principal et étages de retry.

- [ ] **Step 2: Mettre à jour `docs/TASKS.md`**

Cocher P4-T1 à P4-T8, dater au 2026-11-23, lier les commits, coller les sorties de vérification. Tableau de bord : phase 4 à `8 / 8 — terminée`, total `38`. Une ligne de journal par groupe de tâches, dans le style des lignes existantes.

- [ ] **Step 3: Mettre à jour `CLAUDE.md`**

§4 : la clé de `processed_event` devient `(event_id, consumer_group)`, avec la raison en une ligne.
§5 : ajouter les pièges réellement rencontrés pendant l'implémentation. Candidats déjà connus avant d'écrire une ligne de code — à ne garder que s'ils se confirment :
- un `RecordInterceptor` qui marque avant traitement et un chemin de sortie sans exception se combinent en une perte silencieuse ;
- `ExponentialBackOffWithMaxRetries` n'est plus dans spring-core 7.1.0, et `ExponentialBackOff` y a gagné `setMaxAttempts` **et** `setJitter` ;
- une dépendance en portée compile dans `common-kafka` atteint les 6 services, dont 5 n'ont pas de base.

- [ ] **Step 4: Vérifier le tout**

Run: `./mvnw clean verify`
Expected: BUILD SUCCESS, 9 modules. Puis `git status` propre après commit.

- [ ] **Step 5: Commit**

```bash
git add docs CLAUDE.md
git commit -m "docs: fiche pattern 04 et suivi de la phase 4 (P4-T8)"
```

---

## Couverture de la spec

| Section de la spec | Tâche |
|---|---|
| §7.2 `processed_event`, intercepteur, piège du marquage | 2 |
| §7.3 `ErrorHandlingDeserializer`, classement, backoff | 3 et 6 |
| §7.4 étages, résolveur, différé par pause | 4 |
| §7.5 handler unique, trois listeners, injection de panne | 6 |
| §7.6 deux templates, headers de diagnostic | 5 |
| §7.7 endpoint de rejeu et ce qu'il garantit | 8 |
| §7.8 expérience message empoisonné et son témoin | 7 |
| §7.9 dépendances `optional`, `@Import` explicite | 2 et 6 |
| §8 vérifications | une par tâche |
| §9 ordre et commits | les 9 tâches (la 4 et la 5 de la spec sont séparées ici : un relecteur peut rejeter le différé en acceptant la DLT) |

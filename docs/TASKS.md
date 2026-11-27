# TASKS.md — suivi d'avancement

**Source de vérité de l'avancement du projet.** Toute tâche terminée est cochée ici, datée, avec sa commande de vérification exécutée.

Dernière mise à jour : **2026-11-23**

## Légende

| Symbole | État |
|---|---|
| `[ ]` | à faire |
| `[~]` | en cours |
| `[x]` | fait + vérifié |
| `[-]` | abandonné (raison en note) |

Convention d'ID : `P<phase>-T<tâche>`. Les tâches se réfèrent par leur ID dans les commits et les ADR.

---

## Tableau de bord

| Phase | Thème | Tâches | Fait | État |
|---|---|---|---|---|
| 0 | Socle & infra | 9 | 9 | **terminée** |
| 1 | Producer / Consumer fondamentaux | 8 | 8 | **terminée** |
| 2 | Contrats & JSON Schema | 7 | 7 | **terminée** |
| 3 | Fiabilité producer | 6 | 6 | **terminée** |
| 4 | Fiabilité consumer : retry, DLQ, idempotence | 8 | 8 | **terminée** |
| 5 | Outbox transactionnel | 6 | 0 | à faire |
| 6 | Saga chorégraphiée & compensation | 9 | 0 | à faire |
| 7 | Transactions Kafka & exactly-once | 6 | 0 | à faire |
| 8 | Event sourcing & CQRS | 9 | 0 | à faire |
| 9 | Kafka Streams | 9 | 0 | à faire |
| 10 | Stratégie de test | 7 | 0 | à faire |
| 11 | Observabilité & exploitation | 8 | 0 | à faire |
| 12 | Prod-readiness & sécurité | 8 | 0 | à faire |
| 13 | Synthèse & restitution | 5 | 0 | à faire |
| | **Total** | **105** | **38** | |

---

## Phase 0 — Socle & infra

Objectif : un broker KRaft qui tourne, un build multi-module qui passe, de quoi observer.

- [x] **P0-T1** Installer JDK 25, pointer `JAVA_HOME`.
      *Vérif* : Microsoft OpenJDK **25.0.2+10 LTS** déjà présent à
      `C:\Users\mouha\Documents\WorkspaceJAVA\1-MesJDKs\MicrosoftOpenJDKs\jdk-25.0.2+10`.
      Le JDK du `PATH` reste un 21 → `JAVA_HOME` à positionner par session (cf. README).
- [x] **P0-T2** `git init`, `.gitignore` Java/Maven/IDE, premier commit.
      *Vérif* : `git status` propre.
- [x] **P0-T3** Parent `pom.xml` : Boot 4.2.0, BOM Spring Cloud 2026.0.0, `release=25`, repo `packages.confluent.io`, modules déclarés.
      *Vérif* : `./mvnw -N validate`.
- [x] **P0-T4** Maven wrapper 3.9.13 committé.
      *Vérif* : `./mvnw -version`.
- [x] **P0-T5** `infra/docker-compose.yml` : `apache/kafka:4.4.0` en **KRaft** mono-nœud, `confluentinc/cp-schema-registry:8.4.1`, `kafbat/kafka-ui:v1.5.1`, `postgres:18-alpine`.
      *Vérif* : `docker compose up -d --wait` → **4 services healthy**. Schema Registry répond
      `compatibilityLevel = BACKWARD`, Kafka UI renvoie HTTP 200, Postgres expose les 5 bases
      (`orders`, `payments`, `inventory`, `shipping`, `notifications`).
      Deux corrections nécessaires, documentées dans CLAUDE.md §5 : volume Postgres 18 sur
      `/var/lib/postgresql` (et non `.../data`), et préfixe `PLAINTEXT://` — non `INTERNAL://` —
      pour `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS`.
- [x] **P0-T6** Script `infra/topics/create-topics.sh` : tous les topics du tableau de CLAUDE.md §2, partitions et politique de nettoyage incluses.
      *Vérif* : **17 topics créés**. `--describe` confirme
      `shop.order.events.v1` → `PartitionCount: 3, cleanup.policy=delete, retention.ms=604800000`
      et `shop.order.store.v1` → `PartitionCount: 3, cleanup.policy=compact`.
      Variante PowerShell fournie (`create-topics.ps1`). Piège Git Bash corrigé :
      `MSYS_NO_PATHCONV=1`, sinon `docker exec` sort en 127 (cf. CLAUDE.md §5).
- [x] **P0-T7** Modules vides `common-events`, `common-kafka` + squelettes des 6 services, chacun avec Actuator et son port.
      *Vérif* : `./mvnw clean verify` → **BUILD SUCCESS**, 9 modules, 6 tests de contexte verts
      (`Tests run: 1, Failures: 0, Errors: 0` par service), 6 jars Boot produits.
- [x] **P0-T8** `docs/adr/ADR-001-choix-stack.md` : pourquoi KRaft, JSON Schema plutôt qu'Avro, saga chorégraphiée plutôt qu'orchestrée.
- [x] **P0-T9** `docs/patterns/00-pourquoi-event-driven.md` : couplage temporel vs logique, ce que Kafka apporte, ce qu'il coûte.

---

## Phase 1 — Producer / Consumer fondamentaux

Objectif : sentir physiquement partitions, clés, groupes et offsets.

- [x] **P1-T1** `OrderPlaced` en record dans `common-events` avec l'enveloppe complète (`eventId`, `occurredAt`, `correlationId`, …).
      *Vérif* : `./mvnw -pl common-events test` → **11 tests verts**. `EventEnvelope.root()` /
      `causedBy()` testés sur une chaîne de 4 événements. Aller-retour JSON sans perte et
      champs exacts du contrat vérifiés. Commit `cf2e803`.
      Décision : `version` = version du **contrat**, pas la séquence de l'agrégat (champ
      distinct en phase 8). `isRoot()` est `@JsonIgnore` — un accesseur préfixé `is` ajoutait
      un champ `"root"` non déclaré au JSON.
- [x] **P1-T2** `order-service` : `POST /orders` → publication `OrderPlaced` sur `shop.order.events.v1`, clé = `orderId`.
      *Vérif* : `POST /orders` → **202 Accepted**, `{"partition":0,"offset":0}`.
      `kafka-console-consumer --property print.key=true --property print.headers=true`
      affiche `cle=order-7f7f4283-…`, headers `eventId/eventType/correlationId`, et **aucun**
      header `__TypeId__`. Commit `685cefd`.
      Durcissement après revue de sécurité : `@Positive`, `@Digits(7,2)` et bornes de taille
      sur l'entrée (POST avec 2000 décimales → 400). Prix et `customerId` fournis par le
      client restent des limites assumées du labo → **P12-T8**.
- [x] **P1-T3** `notification-service` : consumer `@KafkaListener`, log de l'événement.
      *Vérif* : `partitions assigned: [shop.order.events.v1-0, -1, -2]` au démarrage, puis
      `recu #1 OrderPlaced cle=order-7f7f4283-… partition=0 offset=0`. `GET /stats` →
      `{"received":1,"byPartition":{"0":1}}`. Commit `6cb3252`.
      Le `JacksonJsonDeserializer` reste **nu** jusqu'à la phase 4 : un message illisible
      bloque la partition. Commenté dans l'`application.yml`, c'est la démonstration à faire
      en P4.
- [x] **P1-T4** **Expérience partitions** : publier 100 commandes sur 3 partitions, observer la répartition par clé.
      *Vérif* : `infra/experiments/p1-t4-partitions.ps1` → **33 % / 30 % / 37 %** sur 100 clés
      distinctes. Même clé 5 fois → **toujours la partition 2**. Commit `251f07d`.
      Piège trouvé en route : un BOM UTF-8 ajouté par un pipe PowerShell change la clé
      (`\uFEFForder-A`) et donc la partition, à affichage identique.
- [x] **P1-T5** **Expérience consumer group** : lancer 2 puis 4 instances de `notification-service`, observer l'assignation et le rebalancing.
      *Vérif* : `infra/experiments/p1-t5-rebalancing.ps1` + `--describe` → 1 instance : 3
      partitions ; 2 : 2+1 ; 3 : 1 chacune ; **4 : une instance sans partition, 0 événement**.
      L'instance écartée n'est pas la dernière démarrée. Commit `50066c9`.
- [x] **P1-T6** **Expérience offsets** : reset d'offset sur un groupe, rejeu complet, puis `--to-datetime`.
      *Vérif* : `infra/experiments/p1-t6-offsets.ps1` → reset refusé sur groupe actif
      (`current state is Stable`) ; `--to-earliest` → offsets 0, LAG 86/95/91, **272
      événements retraités** ; `--to-datetime` → offsets 31/39/31, **171 événements**.
      Mesure notable : **47 s** avant que le groupe passe à `Empty` après un `kill` (pas de
      `LeaveGroup`, expiration de `session.timeout.ms`). Commit `251ae8e`.
- [x] **P1-T7** Comparer `@KafkaListener` (spring-kafka) et binding fonctionnel Spring Cloud Stream 5.1.0 sur le même cas ; décider l'usage par module.
      *Vérif* : `infra/experiments/p1-t7-deux-modeles.ps1` → les deux consomment 5/5 ;
      démarrage 4,3 s contre 3,7 s ; 180 contre 599 lignes de log. `./mvnw clean verify` →
      BUILD SUCCESS, 9 modules. Commit `2627259`.
      Décision (ADR-002) : **spring-kafka** pour les 5 services de la saga, **Spring Cloud
      Stream** pour `analytics-service` (Kafka Streams). Le profil `cloud-stream` de
      `notification-service` reste en état de marche comme terrain de comparaison.
- [x] **P1-T8** `docs/patterns/01-partitions-cles-groupes.md` + `docs/adr/ADR-002-spring-kafka-vs-cloud-stream.md`.
      *Vérif* : les deux fiches écrites avec les mesures réelles des expériences T4 à T7, et
      un tableau « comment le casser pour voir l'échec ».

---

## Phase 2 — Contrats & JSON Schema

Objectif : des événements versionnés, validés au broker, qui évoluent sans casser les consumers.

- [x] **P2-T1** JSON Schemas de tous les événements dans `common-events/src/main/resources/schemas/`.
      *Vérif* : `OrderPlaced.json` porte les contraintes que le record Java ne sait pas
      exprimer (`^[A-Z]{3}$` sur la devise, `minItems`, `format: date-time`). Un test vérifie
      que le titre du schéma est bien l'`eventType`, puisque c'est lui qui forme le sujet.
      `subjects.properties` dit sur quel topic chaque schéma circule. Commit `6b38dda`.
- [x] **P2-T2** `KafkaJsonSchemaSerializer` / `Deserializer` 8.4.1 configurés, `auto.register.schemas=false` (enregistrement explicite).
      *Vérif* : `./mvnw -pl common-kafka -am test` → **8 tests verts**. De bout en bout :
      3 `POST /orders` → 3 événements consommés. `od -c` sur le topic montre
      `        004 {` — octet magique 0, id de schéma 4, puis le JSON.
      Commits `6b38dda`, `8fd1a34`.
      `EventJsonSchemaSerializer` impose le schéma du fichier (le schéma dérivé de la classe
      s'appellerait `"Order Placed"`), `json.fail.invalid.schema=true` (défaut `false` : un
      message hors contrat partait sans un mot) et `TopicRecordNameStrategy`.
- [x] **P2-T3** Enregistrement des schémas au build (plugin Maven ou script), avec stratégie de sujet `TopicRecordNameStrategy`.
      *Vérif* : `infra/schemas/register-schemas.ps1` → **4 sujets** (topic de base, retry-1,
      retry-2, DLT), tous `id=4` — les ids de schéma sont globaux, les sujets ne sont que des
      pointeurs. Commit `8fd1a34`.
      Découvert par un test : le nom du topic faisant partie du sujet, les topics dérivés ont
      besoin de leur propre enregistrement, sinon `Error retrieving latest version`.
- [x] **P2-T4** Compatibilité `BACKWARD` imposée sur tous les sujets.
      *Vérif* : `Invoke-RestMethod http://localhost:8081/config/<sujet>` → `BACKWARD` sur les
      4 sujets, et `BACKWARD` en global. Commit `8fd1a34`.
- [x] **P2-T5** **Expérience évolution** : ajouter un champ optionnel → passe. Ajouter un champ requis → rejeté. Documenter les deux.
      *Vérif* : `infra/schemas/p2-t5-evolution.ps1`, sur un sujet de laboratoire supprimé à la
      fin (le contrat réel reste en v1). Commit `6b11f80`.
      A champ optionnel → **compatible** ; B champ requis → refusé
      (`REQUIRED_PROPERTY_ADDED_TO_UNOPEN_CONTENT_MODEL`) ; C retrait d'un champ → refusé
      (`PROPERTY_REMOVED_FROM_CLOSED_CONTENT_MODEL`).
      C n'était pas prévu : c'est le coût d'`additionalProperties: false`. Le modèle fermé
      attrape les fautes de frappe **et** rigidifie l'évolution.
- [x] **P2-T6** Résoudre le conflit Jackson 2 / Jackson 3 si les serdes Confluent le déclenchent.
      *Vérif* : `./mvnw -pl common-kafka dependency:tree` → `tools.jackson.core:jackson-databind:3.1.8`
      et `com.fasterxml.jackson.core:jackson-databind:2.21.8` cohabitent. Commit `6b38dda`.
      Il n'y a pas de conflit de versions, les packages différant. Le vrai problème est que
      les serdes Confluent sérialisent en Jackson 2, qui écrit un `Instant` en timestamp
      décimal là où la phase 1 produisait de l'ISO-8601 : le type d'un champ du contrat
      changeait silencieusement. Corrigé par un mapper Jackson 2 explicite.
- [x] **P2-T7** `docs/patterns/02-schema-registry-evolution.md`.
      *Vérif* : fiche écrite avec les mesures réelles, les codes d'erreur du registre, et les
      trois incidents rencontrés — `Unknown magic byte!` (un changement de format est une
      migration, pas une évolution), `error code: 40403` (supprimer un schéma casse le passé),
      et le format des dates selon la bibliothèque qui sérialise.

---

## Phase 3 — Fiabilité producer

Objectif : savoir exactement ce qu'un `send()` garantit, et ce qu'il ne garantit pas.

- [x] **P3-T1** `acks=all`, `enable.idempotence=true`, `max.in.flight=5`, `retries` — expliquer chaque valeur en commentaire.
      *Vérif* : `order-service/src/main/resources/application.yml`, section `producer` — chaque
      valeur commentée avec ce qu'elle garantit et ce que valent les autres choix. Deux points
      notés : sur un labo mono-nœud `acks=all` ne protège de rien (une seule replica), et
      `max.in.flight=5` ne préserve l'ordre que **grâce** à l'idempotence. Commit `289a516`.
- [x] **P3-T2** Gestion du `CompletableFuture` de `send()` : callback d'échec, pas de fire-and-forget silencieux.
      *Vérif* : `publish()` déballe la `CompletionException`, journalise la cause Kafka réelle
      et lève `EventPublicationFailedException` → **503** avec un message générique.
      `publishAsync()` impose un `whenComplete`. Commit `289a516`.
      Durcissement après revue de sécurité : le 503 ne renvoie plus le message du client
      Kafka, qui nomme l'hôte du broker, celui du registre, le sujet et les timeouts.
- [x] **P3-T3** **Expérience perte de message** : `acks=0`, tuer le broker pendant un envoi en rafale, compter les messages perdus.
      *Vérif* : `infra/experiments/p3-t3-perte.ps1`, 200 000 messages, `docker kill` à 4 s.
      Commit `b641905`.
      `acks=0` → 7 481 déclarés envoyés, **6 231 dans le topic : 1 250 perdus en silence**.
      `acks=all` → 62 769 déclarés, 62 769 stockés, **écart nul**, et 3 978 échecs comptés
      donc visibles.
- [x] **P3-T4** **Expérience duplication** : désactiver l'idempotence, forcer un timeout de retry, constater le doublon.
      *Vérif* : `infra/experiments/p3-t4-doublons.ps1`, `request.timeout.ms=5` pour que
      l'accusé arrive toujours trop tard. Commit `b641905`.
      Sans idempotence : **47 156 records pour 20 000 `eventId` distincts** (+135,8 %). Avec :
      20 000 records, 0 doublon.
      Trois impasses documentées, dont `docker pause` qui ne duplique pas — geler le broker
      l'empêche aussi bien d'écrire que d'accuser.
- [x] **P3-T5** Tuning `linger.ms` / `batch.size` / `compression.type` ; mesurer le débit avant/après.
      *Vérif* : `infra/experiments/p3-t5-debit.ps1`, 3 000 messages, 3 essais, médiane, après
      échauffement. Commit `289a516`.
      **Conclusion inverse de celle attendue** : la dispersion entre essais d'un même scénario
      (11 583 à 26 548 msg/s) dépasse l'écart entre scénarios. Les trois réglages ne sont pas
      mesurables ici ; seule la validation JSON Schema déplace la médiane (x1,48).
      Sans échauffement, le scénario de référence tombait à 773 msg/s et **tous les écarts
      étaient faux**, dans le sens flatteur.
- [x] **P3-T6** `docs/patterns/03-garanties-producer.md`.
      *Vérif* : fiche écrite avec les trois mesures, les quatre horloges du producteur
      (`max.block.ms`, `request.timeout.ms`, `delivery.timeout.ms`, `linger.ms`) et ce que
      l'idempotence producteur ne couvre pas.

---

## Phase 4 — Fiabilité consumer : retry, DLQ, idempotence

Objectif : un consumer qui survit à un message empoisonné et à un rejeu.

- [x] **P4-T1** Table `processed_event` + intercepteur d'idempotence réutilisable dans `common-kafka`.
      *Vérif* : `./mvnw -pl common-kafka -am test` → **64 tests verts**.
      `JdbcProcessedEventStoreIT` sous `-Pintegration` → **7 tests** contre un vrai Postgres
      18.7 (Testcontainers 2.0.5) : premier `insert` 1 ligne affectée, second 0.
      Commits `2d92e16`, `a055544`.
      Clé primaire **composite** `(event_id, consumer_group)`, écart avec CLAUDE.md §4 validé
      par l'utilisateur et §4 corrigé : les 5 services de la saga liront les mêmes topics dans
      5 groupes. `source_topic` hors de la clé, sinon chaque étage de retry rejouerait l'effet.
      Correctif de sécurité `a055544` : `failure()` ne défait que la marque posée par
      `intercept()`. Sans cela, une copie illisible d'un événement effaçait la marque d'un
      travail déjà fait, et la livraison suivante rejouait l'effet métier.
- [x] **P4-T2** Test : rejouer deux fois le même `eventId` → un seul effet métier.
      *Vérif* : `IdempotentRecordInterceptorTest` (10 tests, écrits avant l'implémentation) et
      `RetryChainTest.twoDeliveriesOfTheSameEventIdProduceASingleEffect` de bout en bout sur
      broker embarqué — les **deux** records sont dans le topic, le compteur métier reste à 1.
      C'est le consumer qui déduplique, pas le producteur : l'idempotence producteur de la
      phase 3 ne couvre pas ce cas.
- [x] **P4-T3** `DefaultErrorHandler` + `ExponentialBackOff`, distinction erreurs **retryables** / **non-retryables**.
      *Vérif* : `ErrorClassificationTest` → **8 tests**. Non-retryable → recoverer au 1er échec ;
      transitoire → 3 tentatives puis recoverer. Commit `319aa68`.
      `javap -c` sur `ExceptionClassifier` montre que spring-kafka classe déjà 6 exceptions
      non-retryables par défaut : on les redéclare pour que le classement se **lise**.
      `ExponentialBackOff` de spring-core 7.1.0 et non `ExponentialBackOffWithMaxRetries` —
      seul le premier a `setJitter`.
- [x] **P4-T4** Topics de retry étagés (`retry-1`, `retry-2`) avec délais croissants, puis DLT.
      *Vérif* : `RetryTopologyTest` 9 tests, `RetryStageSupportTest` 11 tests. Horodatages
      réels relevés dans `RetryChainTest` (délais ramenés à 400 ms / 900 ms) :
      échec à `20:15:46.177` → retraité depuis `.retry-1` à `46.752`, depuis `.retry-2` à
      `47.305`. Commits `67daae8`, `a1bbcf5`.
      Le différé **lève** `NotYetDueException` au lieu de sortir normalement : la première
      version faisait `pause` + `seek` + retour normal et **perdait tous les messages en
      attente**, sans une ligne de log (`failure()` n'étant appelé que sur exception, la marque
      d'idempotence restait en place).
      Trouvaille : `ContainerPausingBackOffHandler` met en pause **tout le conteneur**, pas la
      partition — d'où un handler maison et une `TopicPartition` portée par l'exception.
      Correctif de sécurité `a1bbcf5` : `x-origin-timestamp` vient du producteur, donc
      l'attente est bornée par le délai de l'étage (une valeur très future suspendait la
      partition pour des années, une valeur négative débordait).
- [x] **P4-T5** `DeadLetterPublishingRecoverer` : headers de cause, stacktrace, topic/partition/offset d'origine.
      *Vérif* : `DiagnosticHeadersTest` 5 tests, `DeadLetterRoutingTest` 7 tests. En réel,
      `GET /admin/dlt` renvoie `originTopic`, `retryStage`, `exceptionType`, `failedAt`.
      Commit `a38a8c2`.
      `x-origin-topic` et `x-origin-timestamp` sont posés **une seule fois** : les
      `kafka_dlt-original-*` du framework sont réécrits à chaque étage et désignent `.retry-1`
      au bout de deux étages.
- [x] **P4-T6** **Expérience poison message** : publier un payload invalide, vérifier qu'il ne bloque pas la partition.
      *Vérif* : `infra/experiments/p4-t6-poison.ps1`, les deux runs. Commit `956f5a4`.
      **Protégé** : 20 valides publiés, **20 traités**, 1 message en `.dlt`, 0 en `.retry-1`,
      lag 0. **Témoin** (sans `ErrorHandlingDeserializer`) : 20 publiés, **10 traités**, 0 en
      `.dlt`, **lag 11, offset figé à 6641** — celui du message empoisonné.
      `.retry-1` à 0 côté protégé : un échec de désérialisation est non-retryable.
      Cette expérience a seule révélé que `org.flywaydb:flyway-core` **n'applique aucune
      migration** sans `spring-boot-starter-flyway` (auto-configurations découpées en modules
      en Boot 4) — en silence, et chaque événement partait en DLT sur
      `BadSqlGrammarException`.
- [x] **P4-T7** Endpoint d'administration de rejeu depuis la DLT (sélectif, idempotent).
      *Vérif* : `DltReplayTest` 9 tests, puis en réel sur l'infra Docker —
      `{"republished":1}` aux **deux** appels, compteur métier 0 → 1 → **1**.
      Commits `058b4e2`, `7fe711f`.
      La propriété vraie n'est pas « un rejeu est sauté » (faux : la marque a été effacée par
      l'échec) mais **« deux rejeux, un seul effet »**. C'est le consumer qui déduplique.
      Trois garde-fous testés : destination lue dans le **message** (`x-origin-topic`, sinon le
      rejeu republierait sur `.dlt` et bouclerait), bornes du log lues avant de chercher, borne
      dure à 100 offsets. Lecture par `assign` + `seek` **sans `group.id`**, vérifiée par
      `Admin.listGroups()`.
      Deux correctifs : `offsets:[null]` rendait 500 au lieu de 400, et les services sont
      désormais liés au **loopback** — le javadoc affirmait `127.0.0.1` alors que Spring Boot
      écoute sur `0.0.0.0` (CLAUDE.md §1 ne couvrait que les ports Docker).
- [x] **P4-T8** `docs/patterns/04-retry-dlq-idempotence.md`.
      *Vérif* : fiche écrite avec les mesures réelles des trois expériences, les huit façons de
      casser le mécanisme et ce qu'on voit alors, et la comparaison avec `@RetryableTopic`
      (dont le décalage de nommage est mesuré par `javap`). CLAUDE.md §1, §3, §4 corrigés et
      10 pièges ajoutés en §5.

---

## Phase 5 — Outbox transactionnel

Objectif : résoudre le dual-write problem — base et Kafka ne peuvent pas partager une transaction.

- [ ] **P5-T1** **Démontrer le problème** : commit base + crash avant `send()` → événement perdu. Reproductible par test.
- [ ] **P5-T2** Table `outbox` (`id`, `aggregate_id`, `event_type`, `payload`, `headers`, `created_at`, `published_at`).
- [ ] **P5-T3** Écriture état + outbox dans **une seule** transaction JPA.
- [ ] **P5-T4** Publisher par polling (`@Scheduled` + `FOR UPDATE SKIP LOCKED`), au moins une fois.
- [ ] **P5-T5** Purge des lignes publiées + métrique d'âge de la plus vieille ligne non publiée.
- [ ] **P5-T6** `docs/patterns/05-outbox-transactionnel.md` + `docs/adr/ADR-003-outbox-polling-vs-cdc.md` (pourquoi pas Debezium ici).

---

## Phase 6 — Saga chorégraphiée & compensation

Objectif : le cœur du fil rouge. Une transaction métier répartie sur 4 services, sans coordinateur.

- [ ] **P6-T1** Cartographier la saga : états, événements, transitions, compensations. Schéma dans `docs/`.
- [ ] **P6-T2** `payment-service` : consomme `OrderPlaced`, publie `PaymentAuthorized` / `PaymentDeclined`.
- [ ] **P6-T3** `inventory-service` : consomme `PaymentAuthorized`, publie `StockReserved` / `StockUnavailable`.
- [ ] **P6-T4** `shipping-service` : consomme `StockReserved`, publie `ShipmentCreated`.
- [ ] **P6-T5** `order-service` consomme les retours et fait avancer l'agrégat jusqu'à `OrderCompleted`.
- [ ] **P6-T6** Branche de compensation : `StockUnavailable` → `RefundPayment` → `OrderCancelled`.
- [ ] **P6-T7** Timeout de saga : commande bloquée en `PENDING_PAYMENT` au-delà de N minutes → annulation.
- [ ] **P6-T8** Propagation `correlationId` / `causationId` sur toute la chaîne, via headers Kafka.
      *Vérif* : tracer une commande de bout en bout par son `correlationId` dans Kafka UI.
- [ ] **P6-T9** `docs/patterns/06-saga-choregraphiee.md` : comparaison chorégraphie / orchestration, pièges d'ordre partiel.

---

## Phase 7 — Transactions Kafka & exactly-once

Objectif : comprendre ce que l'EOS couvre vraiment, et ce qu'il coûte.

- [ ] **P7-T1** `KafkaTransactionManager` + `transactional.id` sur `inventory-service`.
- [ ] **P7-T2** Pattern consume-process-produce atomique (offsets commités dans la transaction).
- [ ] **P7-T3** `isolation.level=read_committed` côté consumers aval.
- [ ] **P7-T4** **Expérience** : crash au milieu d'une transaction → vérifier qu'aucun aval ne voit le message abandonné.
- [ ] **P7-T5** Mesurer le coût : débit avec et sans transactions.
- [ ] **P7-T6** `docs/patterns/07-transactions-exactly-once.md` : pourquoi l'EOS ne couvre pas les effets de bord externes (mail, appel HTTP).

---

## Phase 8 — Event sourcing & CQRS (greffe projet 3)

Objectif : `order-service` ne stocke plus un état, mais son histoire.

- [ ] **P8-T1** Event store : table `order_events` (`aggregate_id`, `sequence`, `event_type`, `payload`), contrainte d'unicité `(aggregate_id, sequence)`.
- [ ] **P8-T2** Agrégat `Order` reconstruit par `apply()` sur la suite de ses événements.
- [ ] **P8-T3** Contrôle de concurrence optimiste par numéro de séquence attendu.
      *Vérif* : test de deux écritures concurrentes → une seule passe.
- [ ] **P8-T4** Projection read-model `order_summary` alimentée par les événements.
- [ ] **P8-T5** `GET /orders/{id}` lit la projection, jamais l'event store.
- [ ] **P8-T6** Endpoint de reconstruction de projection : purge + rejeu depuis l'offset 0.
      *Vérif* : supprimer la table de projection, rejouer, retrouver le même état.
- [ ] **P8-T7** Snapshots tous les N événements, pour éviter le rejeu intégral.
- [ ] **P8-T8** Versionnement d'événements : `OrderPlaced` v1 → v2 avec upcasting au chargement.
- [ ] **P8-T9** `docs/patterns/08-event-sourcing-cqrs.md` : cohérence à terme, lag de projection, quand ne PAS faire d'event sourcing.

---

## Phase 9 — Kafka Streams

Objectif : traiter les flux, pas les messages un par un.

- [ ] **P9-T1** `analytics-service` : binder Kafka Streams Spring Cloud Stream 5.1.0, topologie minimale.
- [ ] **P9-T2** KStream → agrégation du chiffre d'affaires par fenêtre glissante de 5 min.
- [ ] **P9-T3** KTable du stock courant par SKU, alimentée par `shop.inventory.events.v1` (topic compacté).
- [ ] **P9-T4** Join stream-table : enrichir les commandes avec le stock disponible.
- [ ] **P9-T5** Détection d'anomalie : plus de N commandes du même client en 1 min → `FraudSuspected`.
- [ ] **P9-T6** State store + Interactive Queries exposées en REST.
- [ ] **P9-T7** Comprendre les topics internes générés (`-changelog`, `-repartition`) et leur coût.
      *Vérif* : `kafka-topics.sh --list` avant/après.
- [ ] **P9-T8** `TopologyTestDriver` pour tester la topologie sans broker.
- [ ] **P9-T9** `docs/patterns/09-kafka-streams.md` : event time vs processing time, grace period, messages en retard.

---

## Phase 10 — Stratégie de test

Objectif : tester de l'asynchrone sans `Thread.sleep`.

- [ ] **P10-T1** Tests unitaires de domaine sans Kafka (agrégat, décisions de saga).
- [ ] **P10-T2** `@EmbeddedKafka` pour les tests producer/consumer rapides.
- [ ] **P10-T3** Testcontainers 2.0.5 : broker + Schema Registry + Postgres, profil `integration`.
      *Vérif* : `./mvnw verify -Pintegration`.
- [ ] **P10-T4** Awaitility partout, zéro `Thread.sleep` dans les tests.
      *Vérif* : `Select-String -Pattern "Thread.sleep" -Path **/src/test/**` → aucun résultat.
- [ ] **P10-T5** Test de saga de bout en bout : chemin heureux + chemin de compensation.
- [ ] **P10-T6** Tests de compatibilité de schéma dans la CI (rejet d'un changement cassant).
- [ ] **P10-T7** `docs/patterns/10-tester-asynchrone.md`.

---

## Phase 11 — Observabilité & exploitation

Objectif : savoir ce qui se passe quand ça ne marche pas.

- [ ] **P11-T1** Actuator + métriques Micrometer Kafka exposées en Prometheus.
- [ ] **P11-T2** Prometheus + Grafana dans le compose, dashboard lag / débit / erreurs.
- [ ] **P11-T3** Alerte sur consumer lag au-delà d'un seuil.
- [ ] **P11-T4** Tracing distribué OpenTelemetry : un trace unique couvrant toute la saga.
      *Vérif* : trace complète visible dans Jaeger/Tempo.
- [ ] **P11-T5** Logs structurés JSON avec `correlationId` en MDC, propagé depuis les headers Kafka.
- [ ] **P11-T6** Health indicator custom : broker joignable, lag acceptable, outbox non engorgée.
- [ ] **P11-T7** **Expérience panne** : couper le broker 60 s, observer le comportement des 6 services et leur reprise.
- [ ] **P11-T8** `docs/patterns/11-observabilite.md` : les 5 métriques à regarder en premier.

---

## Phase 12 — Prod-readiness & sécurité

Objectif : ce qui séparerait ce labo d'un vrai déploiement.

- [ ] **P12-T1** Dimensionnement des partitions : raisonnement documenté, pas un chiffre arbitraire.
- [ ] **P12-T2** Compaction : log compacté pour l'event store et les KTable, rétention pour le reste.
- [ ] **P12-T3** Rebalancing : stratégie cooperative-sticky, `max.poll.interval.ms` vs durée de traitement.
      *Vérif* : expérience de rebalancing sous charge.
- [ ] **P12-T4** SASL/SCRAM + TLS dans un profil compose dédié.
- [ ] **P12-T5** ACLs par service : chaque service n'écrit que sur ses topics.
- [ ] **P12-T6** Multi-broker (3 nœuds KRaft), `replication.factor=3`, `min.insync.replicas=2`.
      *Vérif* : tuer un broker, le cluster reste disponible.
- [ ] **P12-T7** `docs/patterns/12-prod-readiness.md`.
- [ ] **P12-T8** Authentifier `POST /orders` : `customerId` lu dans le jeton et non dans le
      corps, prix lu dans un catalogue côté serveur et non fourni par le client.
      *Vérif* : un POST qui annonce un autre `customerId` ou un prix arbitraire est rejeté.
      Origine : revue de sécurité du commit `685cefd`, limites assumées en phase 1 (cf.
      javadoc de `PlaceOrderRequest`).

---

## Phase 13 — Synthèse & restitution

- [ ] **P13-T1** Index de tous les patterns dans `docs/patterns/README.md`.
- [ ] **P13-T2** Fiche de révision : chaque pattern en 5 lignes, problème → solution → piège.
- [ ] **P13-T3** Antipatterns rencontrés et pourquoi ils échouent.
- [ ] **P13-T4** Schéma d'architecture final, à jour du code réel.
- [ ] **P13-T5** Checklist de revue : que vérifier dans n'importe quel projet Kafka.

---

## Journal

| Date | Phase | Tâches | Note |
|---|---|---|---|
| 2026-11-21 | — | — | Création du repo : CLAUDE.md, README.md, TASKS.md. Versions de la stack vérifiées sur Maven Central et Docker Hub. |
| 2026-11-21 | 0 | P0-T1 à T4, T7 à T9 | Socle en place : git, parent POM, wrapper 3.9.13, 9 modules, ADR-001, fiche pattern 00. `./mvnw clean verify` vert sous JDK 25.0.2+10. |
| 2026-11-21 | 0 | P0-T5, P0-T6 | Infra vérifiée après redémarrage de Docker Desktop : 4 conteneurs healthy, 17 topics créés avec les bonnes politiques de nettoyage, 5 bases Postgres. Trois pièges corrigés et documentés (volume Postgres 18, préfixe bootstrap du Schema Registry, `MSYS_NO_PATHCONV`). |
| 2026-11-21 | 0 | — | **Phase 0 terminée (9/9).** Dépôt public : https://github.com/mouhamadoulo/manage-events-kafka |
| 2026-11-22 | 1 | P1-T1 à P1-T3 | Premier flux de bout en bout : `OrderPlaced` avec enveloppe, `POST /orders` → topic clé `orderId`, consumer `@KafkaListener`. Deux revues de sécurité traitées : bornes de validation ajoutées, prix/authentification reportés en P12-T8, poison message laissé visible pour la phase 4. |
| 2026-11-22 | 1 | P1-T4 à P1-T6 | Trois expériences scriptées dans `infra/experiments/` : répartition des clés (33/30/37 %), rebalancing (4 instances pour 3 partitions → une inactive), offsets (rejeu complet 272 événements, partiel 171, 47 s d'expiration de session). |
| 2026-11-22 | 1 | P1-T7, P1-T8 | Le même consumer écrit en Spring Cloud Stream 5.1.0 sous profil `cloud-stream` ; décision par module dans ADR-002. Fiche `01-partitions-cles-groupes.md`. **Phase 1 terminée (8/8).** |
| 2026-11-22 | 2 | P2-T1, P2-T2, P2-T6 | Le contrat devient un fichier JSON Schema versionné, publié via le mécanisme d'enveloppe de Confluent plutôt que dérivé de la classe. Trois défauts de configuration corrigés explicitement (`auto.register.schemas`, `json.fail.invalid.schema`, stratégie de sujet) et le format des dates rétabli en ISO-8601. |
| 2026-11-22 | 2 | P2-T3, P2-T4 | Enregistrement explicite par script : 4 sujets en `BACKWARD`. Deux pièges PowerShell 5.1 trouvés en route — `ConvertTo-Json` qui se bloque sans rien émettre, et `Get-Content` qui lit en ANSI et double-encodait le contrat publié. |
| 2026-11-22 | 3 | P3-T1, P3-T2, P3-T5 | Producteur configuré valeur par valeur, résultat du `send()` regardé (503 au lieu d'un échec muet), et outil de rafale du profil `lab`. Le tuning réseau s'est révélé non mesurable sur ce labo : seule la validation du schéma déplace le débit. |
| 2026-11-22 | 3 | P3-T3, P3-T4, P3-T6 | Les deux pannes mesurées : `acks=0` perd 1 250 records sans un mot, et sans idempotence le réessai écrit +135,8 % de records. Fiche `03-garanties-producer.md`. **Phase 3 terminée (6/6).** |
| 2026-11-22 | 2 | P2-T5, P2-T7 | Expérience de compatibilité : champ optionnel accepté, champ requis refusé, retrait refusé à cause du modèle de contenu fermé. Trois incidents réels documentés dans la fiche 02. **Phase 2 terminée (7/7).** |
| 2026-11-23 | 4 | P4-T1 à P4-T5 | Mécanique de fiabilité consumer dans `common-kafka`, importée explicitement par les services : idempotence par `processed_event` (clé composite), classement retryable / non-retryable, étages de retry dont le différé **lève** une exception, DLT à deux templates. Deux correctifs de sécurité sur des pertes silencieuses : `failure()` qui effaçait la marque d'un autre traitement, et une attente non bornée calculée depuis un header non fiable. |
| 2026-11-23 | 4 | P4-T6, P4-T7 | Les deux pannes mesurées : sans `ErrorHandlingDeserializer`, 10 messages sur 20 traités et l'offset figé ; avec, 20 sur 20 et 1 message en DLT. Rejeu sélectif depuis la DLT, vérifié en réel : deux rejeux, un seul effet métier. Trois bugs que seuls les tests de bout en bout et l'expérience ont révélés — un `KafkaTemplate` déclaré qui supprime celui de Boot, `flyway-core` qui n'applique aucune migration, et les services liés à `0.0.0.0`. |
| 2026-11-23 | 4 | P4-T8 | Fiche `04-retry-dlq-idempotence.md`, CLAUDE.md §1/§3/§4 corrigés, 10 pièges ajoutés en §5. `./mvnw clean verify` → 9 modules, **109 tests**. **Phase 4 terminée (8/8).** |
| 2026-11-24 | 4 | revue de branche | Relecture à contexte frais des 12 commits : aucun Critical, 5 Important, 10 Minor. Les cinq Important étaient du même genre — un garde-fou qui vérifiait la forme et pas le fond, tous sur le chemin d'administration : lecture non bornée dans le rejeu (la borne de 100 offsets ne protégeait rien), `x-origin-topic` cru sans être validé (republication dans la DLT elle-même possible), partition inexistante qui bloquait 60 s, vue sans partition, et trois délais configurés qui routaient vers un `.retry-3` inexistant — une partition bloquée. Corrigés en une passe, 9 tests écrits d'abord. Plus une fuite trouvée en route : le `ProducerFactory` du template `byte[]` n'était jamais fermé. `./mvnw clean verify` → **118 tests**. |

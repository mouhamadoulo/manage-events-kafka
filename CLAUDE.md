# CLAUDE.md — manage-events-kafka

Projet pédagogique : maîtriser l'**Event-Driven Architecture** avec **Apache Kafka** et **Spring Boot**.
Fil rouge : commande e-commerce distribuée (saga chorégraphiée) + greffe **event sourcing / CQRS** sur `order-service`.

Ce fichier est le contrat de travail pour tout agent intervenant sur le repo. Le suivi d'avancement vit dans [docs/TASKS.md](docs/TASKS.md).

---

## 1. Stack — versions figées

Versions vérifiées sur Maven Central / Docker Hub le **2026-11-21**. Ne pas les changer sans mettre à jour ce tableau ET `docs/TASKS.md`.

| Composant | Version | Source / note |
|---|---|---|
| Java | **25** (LTS) | `maven.compiler.release=25` |
| Spring Boot | **4.2.0** | parent POM |
| Spring Framework | **7.1.0** | transitif Boot 4.2.0 |
| Spring Cloud | **2026.0.0** | BOM `spring-cloud-dependencies`, aligné Boot 4.2.0 |
| Spring Cloud Stream | **5.1.0** | géré par le BOM Spring Cloud |
| Spring for Apache Kafka | **4.2.0** | géré par Boot |
| kafka-clients / kafka-streams | **4.4.0** | géré par Boot (`${kafka.version}`) |
| Jackson | **3.1.8** | géré par Boot — voir §5 |
| Testcontainers | **2.0.5** | géré par Boot — API v2, voir §5 |
| Maven | 3.9.13 | wrapper à committer |
| Broker (Docker) | `apache/kafka:4.4.0` | mode **KRaft**, pas de ZooKeeper |
| Schema Registry | `confluentinc/cp-schema-registry:8.4.1` | JSON Schema |
| Kafka UI | `kafbat/kafka-ui:v1.5.1` | ex-provectuslabs |
| Sérialiseurs JSON Schema | `io.confluent:kafka-json-schema-serializer:8.4.1` | repo `https://packages.confluent.io/maven/` |
| Postgres (Docker) | `postgres:18-alpine` | un schéma par service |

### Choix d'architecture non négociables

- **Pas d'Avro.** Sérialisation = **JSON Schema** + Confluent Schema Registry.
- **KRaft uniquement.** Aucune référence à ZooKeeper dans les compose, configs ou docs.
- **Saga chorégraphiée**, pas d'orchestrateur central (pas de Camunda / Temporal).
- **Outbox transactionnel** pour toute publication issue d'un changement d'état en base.
- Projet **Maven multi-module**, un module = un service déployable.
- **Tout port est lié à `127.0.0.1`** — ceux publiés par Docker *et* ceux des services Spring (`server.address`, surchargeable par `SERVER_ADDRESS`). Spring Boot écoute par défaut sur `0.0.0.0` : la règle ne tenait donc que pour Docker jusqu'en P4-T7, alors que `POST /orders`, `/lab/producer/*` et `/admin/dlt/replay` écrivent tous dans des topics métier sans authentification. L'infra locale est entièrement en PLAINTEXT, sans authentification, et Kafka UI tourne avec `DYNAMIC_CONFIG_ENABLED`. Sans bind loopback, n'importe quelle machine du réseau local pourrait lire et écrire dans les topics. Ne jamais republier un port sur `0.0.0.0` — l'authentification arrive en phase 12.

---

## 2. Carte des modules

```
manage-events-kafka/
├── pom.xml                    # parent, dependencyManagement, versions
├── common-events/             # contrats : records d'événements + JSON Schemas (src/main/resources/schemas)
├── common-kafka/              # config partagée : serdes, error handlers, DLQ, idempotence, observabilité
├── order-service/             # agrégat Order : event sourcing + CQRS + outbox. Démarre la saga.
├── payment-service/           # autorise / refuse le paiement. Participant saga.
├── inventory-service/         # réserve / libère le stock. Exactly-once + state store.
├── shipping-service/          # crée l'expédition. Participant saga.
├── notification-service/      # consumer terminal, idempotent, pur read-side.
├── analytics-service/         # Kafka Streams : agrégations fenêtrées, détection d'anomalies (greffe projet 3)
├── infra/
│   ├── docker-compose.yml     # kafka, schema-registry, kafka-ui, postgres
│   └── topics/                # scripts de création de topics (partitions, réplication, compaction)
└── docs/
    ├── TASKS.md               # suivi d'avancement — SOURCE DE VÉRITÉ
    ├── adr/                   # décisions d'architecture (ADR-001, 002, ...)
    └── patterns/              # une fiche par pattern EDA appris
```

### Topics — convention de nommage

`<domaine>.<agrégat>.<type>.v<version>`

| Topic | Clé | Partitions | Nettoyage |
|---|---|---|---|
| `shop.order.events.v1` | `orderId` | 3 | delete (7j) |
| `shop.order.store.v1` | `orderId` | 3 | **compact** (event store) |
| `shop.payment.events.v1` | `orderId` | 3 | delete |
| `shop.inventory.events.v1` | `sku` | 3 | delete |
| `shop.shipping.events.v1` | `orderId` | 3 | delete |
| `shop.<x>.events.v1.dlt` | idem source | 1 | delete (30j) |
| `shop.<x>.events.v1.retry-<n>` | idem source | 3 | delete |

**Règle d'or** : la clé de partition détermine l'ordre. Tout événement du même agrégat porte la même clé. Jamais de clé nulle sur un topic métier.

---

## 3. Commandes

```powershell
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml ps
docker compose -f infra/docker-compose.yml down -v
```

```powershell
./mvnw clean verify
```

```powershell
./mvnw -pl order-service -am clean verify
```

Lancer un service **exige un `install` préalable** : `clean verify` ne publie rien dans le dépôt
local, donc `-pl <module>` ne trouve pas les modules frères et sort sur
`DependencyResolutionException`.

```powershell
./mvnw install -DskipTests
```

```powershell
./mvnw -pl order-service spring-boot:run -Dspring-boot.run.profiles=lab
```

```powershell
./mvnw test
```

```powershell
./mvnw verify -Pintegration
```

Kafka CLI (dans le conteneur) :

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
```

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic shop.order.events.v1 --from-beginning --property print.key=true
```

### URLs locales

| Service | URL |
|---|---|
| Kafka broker | `localhost:9092` |
| Schema Registry | http://localhost:8081 |
| Kafka UI | http://localhost:8080 |
| order-service | http://localhost:8091 |
| payment-service | http://localhost:8092 |
| inventory-service | http://localhost:8093 |
| shipping-service | http://localhost:8094 |
| notification-service | http://localhost:8095 |
| analytics-service | http://localhost:8096 |

---

## 4. Conventions de code

- **Événements** = `record` Java immuable dans `common-events`, jamais d'entité JPA exposée dans un topic.
- Chaque événement porte une **enveloppe** : `eventId` (UUID), `eventType`, `occurredAt`, `correlationId`, `causationId`, `aggregateId`, `version`.
- **Idempotence consumer obligatoire** : table `processed_event`, clé primaire **composite**
  `(event_id, consumer_group)`, insert avant traitement, `ON CONFLICT DO NOTHING` → skip.
  La clé est composite parce que les 5 services de la saga lisent les mêmes topics dans 5
  groupes différents : avec `event_id` seul, le premier service à traiter un événement le
  marquerait pour tous les autres. `source_topic` est journalisé mais **hors de la clé** — un
  record venu de `.retry-1` est le même événement, et l'y mettre ferait rejouer l'effet à
  chaque étage. Corollaire : la marque est posée **avant** le traitement, donc tout chemin de
  sortie du listener doit **lever** (cf. `docs/patterns/04-retry-dlq-idempotence.md`).
- **Nommage** : événements au **passé** (`OrderPlaced`, `PaymentAuthorized`), commandes à l'**impératif** (`ReserveStock`). Un topic d'événements ne transporte pas de commandes.
- **Pas de Lombok** sur les records ; Lombok toléré sur les entités JPA uniquement.
- Config dans `application.yml`, hosts/secrets via variables d'env avec défaut local.
- `spring.kafka.consumer.auto-offset-reset: earliest` en dev.
- Un package par couche : `domain`, `application`, `infrastructure.kafka`, `infrastructure.persistence`, `api`.

### Pédagogie — exigence spécifique à ce repo

Ce projet sert à **apprendre**. Donc :

1. Chaque phase terminée produit une fiche `docs/patterns/<NN>-<pattern>.md` : le problème, le mécanisme Kafka utilisé, le code clé, comment le casser pour voir l'échec, comment le vérifier.
2. Chaque décision structurante produit un ADR dans `docs/adr/`.
3. Le code des patterns porte des commentaires `// WHY:` expliquant le choix, pas le fonctionnement.
4. Toute propriété Kafka non évidente dans un `application.yml` est commentée avec son effet et son défaut.

---

## 5. Pièges connus de cette stack

- **Jackson 3** : packages `tools.jackson.databind.*` (plus `com.fasterxml.jackson.databind.*`). `ObjectMapper` immuable, construit via `JsonMapper.builder()`. Les sérialiseurs Confluent 8.4.1 peuvent encore dépendre de Jackson 2 → vérifier `./mvnw dependency:tree` et aligner.
- **`send()` dont on ignore le `CompletableFuture` perd en silence.** Rien a lire a la relecture : ni exception, ni log, ni metrique. Tout envoi passe par un `whenComplete` ou un `join()` dont l'echec est traite.
- **Le producteur refuse de demarrer si `delivery.timeout.ms < linger.ms + request.timeout.ms`.** Se declenche vite quand on bricole `linger.ms` pour mesurer le debit.
- **`max.block.ms` (defaut 60 s) est le temps que `send()` passe A BLOQUER quand le buffer est plein.** Broker mort, le buffer de 32 Mo se remplit en quelques milliers de messages et l'application parait gelee alors qu'elle fait exactement ce qu'on lui a demande.
- **`max.in.flight > 1` ne preserve l'ordre que GRACE a l'idempotence** (le broker reordonne par numero de sequence). Sans idempotence, toute valeur superieure a 1 permet qu'un lot reessaye passe apres le suivant : l'ordre de la partition, seule garantie d'ordre de Kafka, est casse.
- **`docker pause` ne provoque pas de doublon.** Geler le broker l'empeche aussi bien d'ecrire que d'accuser : il reprend ou il en etait. Le doublon exige une ecriture reussie ET un accuse perdu, donc un timeout CLIENT (`request.timeout.ms` tres court), pas une panne serveur.
- **Mesurer un reglage de producteur sans echauffement ni repetition donne des conclusions inversees.** La premiere rafale paie le JIT et la resolution du schema (773 msg/s contre 20 408 ensuite) ; et la dispersion entre essais d'un meme scenario depasse souvent l'ecart entre scenarios.
- **`Start-Job` demarre un processus PowerShell neuf, en une a deux secondes.** Toute experience qui doit agir « pendant » une rafale courte mesure en fait ce demarrage.
- **Les serdes Confluent 8.4.1 serialisent en Jackson 2**, Spring Boot 4 en Jackson 3. Pas de conflit de versions (packages differents), mais pas le meme resultat : sans module java.time enregistre, Jackson 2 ecrit un `Instant` en timestamp decimal (`1795344536.104638100`) la ou Jackson 3 ecrit de l'ISO-8601. Un passage au Schema Registry change donc silencieusement le type d'un champ du contrat (`string` vers `number`). Convertir l'evenement avec un `ObjectMapper` Jackson 2 explicite (`JavaTimeModule`, `WRITE_DATES_AS_TIMESTAMPS` desactive).
- **`json.fail.invalid.schema` vaut `false` par defaut.** Un message hors contrat part sans un mot. A mettre a `true` cote producteur ET consumer. Corollaire : la validation est faite par le **client**, jamais par le broker (qui ne lit pas les messages) ni par le registre (qui ne voit que des schemas).
- **Ne jamais laisser Confluent deriver le schema d'une classe.** La derivation de `OrderPlaced` donne un titre `"Order Placed"` (avec une espace, donc un sujet `shop.order.events.v1-Order Placed`), enveloppe chaque champ dans un `oneOf [null, ...]` et perd toute contrainte metier. Ecrire le schema a la main et le passer par `JsonSchemaUtils.envelope(schema, payload)`.
- **Avec `TopicRecordNameStrategy`, le nom du topic fait partie du sujet.** Les topics derives (`.retry-1`, `.dlt`) ont donc besoin de leur propre enregistrement, sinon la publication echoue sur `Error retrieving latest version`.
- **Supprimer un sujet avec `?permanent=true` casse le passe.** L'id du schema est inscrit dans chaque message pour toute la duree de retention : les consumers echouent ensuite sur `Schema not found; error code: 40403`. Et un producteur deja lance garde l'ancien id en cache, donc le redemarrer fait partie de l'operation.
- **Changer de format de transport n'est pas une evolution de schema.** Passer du JSON nu au format Confluent (`[0x00][id sur 4 octets][JSON]`) rend les anciens messages illisibles : `Unknown magic byte!`, puis partition bloquee faute d'`ErrorHandlingDeserializer`. Cela se traite par un topic `.v2` publie en parallele, ou par une purge assumee.
- **`ConvertTo-Json` de PowerShell 5.1 se bloque indefiniment** sur une chaine de quelques kilo-octets imbriquee dans un objet (`@{ schema = $texte } | ConvertTo-Json`). Aucune erreur, aucune requete emise : le serveur ne voit rien passer, ce qui fait d'abord soupconner le reseau. Echapper la chaine a la main.
- **`Get-Content` de PowerShell 5.1 lit dans la page de code ANSI**, pas en UTF-8. Un fichier UTF-8 contenant un signe section arrive double-encode dans ce qu'on publie. Toujours `-Encoding UTF8` quand le contenu lu part sur le reseau.
- **spring-kafka 4.2 embarque DEUX familles de serdes JSON.** `JacksonJsonSerializer` / `JacksonJsonDeserializer` utilisent Jackson 3 (`tools.jackson`) ; `JsonSerializer` / `JsonDeserializer`, sans préfixe, sont restés sur Jackson 2 (`com.fasterxml`). Les noms sans préfixe sont ceux de tous les tutoriels existants, et ce sont les mauvais ici. Vérifier par `javap -constants` plutôt que par mémoire.
- **Jackson 3 : les annotations n'ont pas changé de package.** `databind` est passé à `tools.jackson.databind`, mais `jackson-annotations` reste en `com.fasterxml.jackson.annotation` (artefact `com.fasterxml.jackson.core:jackson-annotations:2.21`, tiré transitivement par `tools.jackson.core:jackson-databind:3.1.8`). Donc `import com.fasterxml.jackson.annotation.JsonIgnore;` dans un projet Jackson 3.
- **Un accesseur dérivé fuit dans le JSON d'un record.** Une méthode préfixée `is`/`get` sur un record d'événement devient une propriété Jackson : `isRoot()` ajoutait un champ `"root": true` au message publié. `@JsonIgnore`, ou nommer sans préfixe de getter. À vérifier par un test qui assert la liste exacte des champs — sinon le champ fantôme part dans le Schema Registry.
- **PowerShell 5.1 ajoute un BOM UTF-8 quand on pipe vers un exécutable natif.** `$lignes | docker exec -i … kafka-console-producer.sh` produit une première clé `﻿order-A`, qui part sur une autre partition que `order-A` pour un affichage identique. Passer par un fichier (`Set-Content -Encoding ascii` puis `docker cp` et redirection dans le conteneur).
- **Rediriger stderr d'un exécutable natif (`2>$null`, `2>&1`) en PowerShell 5.1** transforme chaque ligne en `ErrorRecord` et stoppe le script sous `$ErrorActionPreference = "Stop"`. Les outils Kafka écrivent des avertissements sur stderr en fonctionnement normal : passer `$ErrorActionPreference = "Continue"` autour de ces appels.
- **Un consumer tué de force reste membre de son groupe ~45 s** (`session.timeout.ms`), faute de `LeaveGroup`. Pendant ce temps `--reset-offsets` est refusé (`group is inactive` attendu, `Stable` constaté) et les partitions ne sont pas réattribuées. Attendre l'état `Empty` (`--describe --state`), pas un `sleep` fixe.
- **`org.flywaydb:flyway-core` seul n'applique AUCUNE migration.** Spring Boot 4 découpe ses auto-configurations en modules : `FlywayAutoConfiguration` vit dans `spring-boot-flyway`. Avec `flyway-core` seul, le contexte démarre **sans un mot**, la table n'existe pas, et l'erreur n'apparaît qu'au premier accès — ici chaque événement partait en DLT sur `BadSqlGrammarException`. Déclarer `spring-boot-starter-flyway`. Un test d'intégration qui lance Flyway par programme ne peut pas attraper ce défaut : garder un test qui vérifie la présence de la classe d'auto-configuration.
- **Testcontainers 2.0.5** : API majeure v2, signatures de `KafkaContainer` changées vs 1.x. Ne pas copier-coller des exemples 1.19/1.21. Les **artefacts sont renommés** : `org.testcontainers:testcontainers-postgresql` et `testcontainers-junit-jupiter`, et non `postgresql` / `junit-jupiter` — ces derniers ne sont plus gérés par le BOM, et Maven sort sur « `dependencies.dependency.version` is missing ». La classe a aussi déménagé : `org.testcontainers.postgresql.PostgreSQLContainer`, non générique, remplace `org.testcontainers.containers.PostgreSQLContainer<SELF>`.
- **Déclarer un bean `KafkaTemplate` supprime celui de Boot.** `KafkaAutoConfiguration.kafkaTemplate` est `@ConditionalOnMissingBean(KafkaTemplate.class)`. Un module qui déclare un template spécialisé (un `ByteArraySerializer` pour la DLT) prive donc tout le service du template normal — et un `DeadLetterPublishingRecoverer` configuré avec une map de deux templates reçoit alors le même objet pour les deux types de payload : `SerializationException: Can't convert value of class OrderPlaced to class ByteArraySerializer`, en boucle, aucun message n'atteint la DLT. Déclarer les deux templates explicitement.
- **Flyway écarte une location qui est la sous-location d'une autre.** `locations: classpath:db/migration/shared,classpath:db/migration` donne `WARN Discarding location 'classpath:db/migration/shared' as it is a sub-location of 'classpath:db/migration'` : les deux se confondent, le script s'appelle `shared/V1__…`, et une migration locale nommée `V1__` entre en **collision de version**. Emplacements **frères**, et versions 1–99 réservées au partagé.
- **`ContainerPausingBackOffHandler` met en pause TOUT le conteneur, pas la partition.** Il appelle `pauseService.pause(container, durée)`. Sur un `.retry-1` à 3 partitions, un seul record arrivé trop tôt retarde donc les deux autres partitions. Le handler du framework n'a aucun moyen de faire mieux : `BackOffHandler.onNextBackOff(container, exception, interval)` ne reçoit **pas le record**. Pour une pause par partition, faire porter la `TopicPartition` par l'exception — c'est le seul véhicule disponible.
- **Un `RecordInterceptor` qui marque avant traitement et un chemin de sortie sans exception se combinent en une perte silencieuse.** `failure()` n'est appelé que sur exception : un listener qui sort normalement sans avoir travaillé laisse la marque en place, et au passage suivant le record est classé doublon et sauté. Et symétriquement, `failure()` ne doit défaire **que** ce que `intercept()` a posé : sinon un record passé sans déduplication mais portant un `eventId` déjà traité efface la marque d'un travail déjà fait.
- **`ExponentialBackOffWithMaxRetries` n'est plus dans spring-core 7.1.0** (il reste dans `org.springframework.kafka.support`). `org.springframework.util.backoff.ExponentialBackOff` y a gagné `setMaxAttempts(long)` **et** `setJitter(long)` : c'est celui-là qu'il faut, le jitter évitant que N instances du même groupe réessaient à la même milliseconde.
- **`-Dtest=X` avec `-am` fait échouer les modules amont** sur « No tests matching pattern ». Ajouter `-Dsurefire.failIfNoSpecifiedTests=false`. Et `-pl <module>` sans `-am` échoue tout court tant que les modules frères ne sont pas installés (cf. §3).
- **Un caractère U+FEFF *à l'intérieur* d'un script PowerShell casse l'analyseur**, avec un message qui pointe la dernière accolade du fichier (`Jeton inattendu « } »`) et non la vraie ligne. Arrive en écrivant un commentaire *au sujet* du BOM si l'outil d'écriture interprète `﻿`. Les scripts du dépôt sont écrits en **UTF-8 avec BOM** (PS 5.1 lit sinon dans la page ANSI).
- **`ConsumerGroupMetadata` est dépréciée pour suppression en kafka-clients 4.4**, ses deux constructeurs compris. Dans un test, la simuler plutôt que d'en appeler un.
- **Spring Boot 4** : starters repackagés, `@ConfigurationProperties` plus stricts, suppression des API dépréciées de Boot 3. Aucun tutoriel Boot 2/3 appliqué tel quel.
- **Spring Cloud Stream 5.1.0** : modèle fonctionnel (`java.util.function.Function/Consumer/Supplier`) uniquement. `@StreamListener` / `@EnableBinding` n'existent plus.
- **Kafka 4.x** : protocole consumer group **KIP-848** par défaut côté broker. Pas de ZooKeeper.
- **Confluent Schema Registry** absent de Maven Central → déclarer le repo `https://packages.confluent.io/maven/` dans le parent POM.
- **Postgres 18+** : le volume doit être monté sur `/var/lib/postgresql`, **pas** sur `/var/lib/postgresql/data`. L'image place les données dans un sous-dossier nommé par version majeure (pour `pg_upgrade --link`). Monter `.../data` fait sortir le conteneur en code 1 avec un message sur `pg_ctlcluster`. Tous les exemples antérieurs à Postgres 18 montent `.../data` et sont donc faux ici.
- **Schema Registry** : `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS` attend un préfixe de **protocole de sécurité** (`PLAINTEXT://`), pas un nom de listener. `INTERNAL://kafka:19092` échoue avec `No supported Kafka endpoints are configured`, alors que `PLAINTEXT://kafka:19092` cible bien le listener INTERNAL par son port.
- **Git Bash sur Windows** : MSYS réécrit tout argument commençant par `/` en chemin Windows. `docker exec kafka /opt/kafka/bin/kafka-topics.sh` devient `C:/Program Files/Git/opt/...` et sort en code 127. Remède : `export MSYS_NO_PATHCONV=1` (déjà dans `infra/topics/create-topics.sh`), ou doubler le slash (`//opt/...`). En PowerShell, le problème ne se pose pas.
- **Pas de `curl` dans les conteneurs** `apache/kafka` ni `cp-schema-registry` (seulement `wget` et `nc` côté Kafka). Interroger le Schema Registry depuis l'hôte (`Invoke-RestMethod http://localhost:8081/subjects`), pas via `docker exec`.
- **JDK local** : Java 25 est installé à `C:\Users\mouha\Documents\WorkspaceJAVA\1-MesJDKs\MicrosoftOpenJDKs\jdk-25.0.2+10` (Microsoft OpenJDK 25.0.2+10 LTS). Le JDK par défaut du `PATH` est un **21.0.10**, qui échoue avec `error: release version 25 not supported`. Donc : positionner `JAVA_HOME` sur le JDK 25 avant tout build.

  ```powershell
  $env:JAVA_HOME = "C:\Users\mouha\Documents\WorkspaceJAVA\1-MesJDKs\MicrosoftOpenJDKs\jdk-25.0.2+10"
  ```

---

## 6. Règles de travail pour l'agent

1. **Lire `docs/TASKS.md` avant toute action.** Travailler la tâche en cours, pas en avance de phase.
2. **Mettre à jour `docs/TASKS.md`** à chaque tâche terminée : cocher, dater, lier le commit.
3. **Ne jamais inventer une version.** Vérifier sur Maven Central / Docker Hub, puis mettre à jour le tableau §1.
4. **Un commit par tâche**, Conventional Commits, scope = module (`feat(order-service): ...`).
5. **Tests avant implémentation** sur toute logique de fiabilité (idempotence, retry, DLQ, saga, EOS).
6. **Vérifier avant d'affirmer** : une tâche est "faite" seulement après exécution réelle de sa commande de vérification. Coller la sortie dans le rapport.
7. **Pas de raccourci qui masque le pattern.** Ne pas remplacer l'outbox par un `send()` direct "pour aller plus vite" — l'objectif est d'apprendre le pattern.

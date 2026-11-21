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

```powershell
./mvnw -pl order-service spring-boot:run
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
- **Idempotence consumer obligatoire** : table `processed_event (event_id PK, consumer_group, processed_at)`, insert avant traitement, `ON CONFLICT DO NOTHING` → skip.
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
- **Testcontainers 2.0.5** : API majeure v2, signatures de `KafkaContainer` changées vs 1.x. Ne pas copier-coller des exemples 1.19/1.21.
- **Spring Boot 4** : starters repackagés, `@ConfigurationProperties` plus stricts, suppression des API dépréciées de Boot 3. Aucun tutoriel Boot 2/3 appliqué tel quel.
- **Spring Cloud Stream 5.1.0** : modèle fonctionnel (`java.util.function.Function/Consumer/Supplier`) uniquement. `@StreamListener` / `@EnableBinding` n'existent plus.
- **Kafka 4.x** : protocole consumer group **KIP-848** par défaut côté broker. Pas de ZooKeeper.
- **Confluent Schema Registry** absent de Maven Central → déclarer le repo `https://packages.confluent.io/maven/` dans le parent POM.
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

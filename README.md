# manage-events-kafka

Laboratoire d'apprentissage de l'**Event-Driven Architecture** avec **Apache Kafka** et **Spring Boot 4**.

Le projet n'est pas une démo jetable : c'est un fil rouge qui monte en complexité phase par phase, chaque phase introduisant un pattern EDA réel avec son code, ses tests et sa fiche d'explication.

---

## Le fil rouge

Une **commande e-commerce distribuée**, traitée par cinq services qui ne se parlent que par événements Kafka. Aucun appel REST entre services.

```
                                   shop.order.events.v1
   POST /orders ──▶ order-service ──────────────┬────────────────────────────┐
                    (event sourcing             │                            │
                     + outbox)                  ▼                            ▼
                                        payment-service            notification-service
                                               │
                                shop.payment.events.v1
                                               │
                                               ▼
                                       inventory-service
                                               │
                              shop.inventory.events.v1
                                               │
                                               ▼
                                       shipping-service
                                               │
                              shop.shipping.events.v1
                                               │
                                               ▼
                                        order-service
                                     (OrderCompleted)

   Tous les topics ──▶ analytics-service (Kafka Streams : agrégations, anomalies)
```

### Le chemin heureux

1. `order-service` reçoit `POST /orders`, persiste les événements de l'agrégat, publie `OrderPlaced` via **outbox**.
2. `payment-service` consomme, autorise, publie `PaymentAuthorized`.
3. `inventory-service` consomme, réserve le stock en **exactly-once**, publie `StockReserved`.
4. `shipping-service` consomme, publie `ShipmentCreated`.
5. `order-service` consomme et clôt l'agrégat : `OrderCompleted`.

### Le chemin de compensation (le vrai sujet)

`PaymentDeclined` ou `StockUnavailable` déclenche la **saga chorégraphiée inverse** : `ReleaseStock`, `RefundPayment`, `OrderCancelled`. C'est là que vivent les patterns durs — idempotence, ordre, rejeu, DLQ.

### La greffe event sourcing / CQRS

`order-service` ne stocke pas un état mais la **suite de ses événements**. L'état courant est reconstruit par rejeu. Un read-model séparé (projection) sert les requêtes `GET /orders/{id}`. Permet de voir en vrai : rejeu depuis l'offset 0, reconstruction de projection, snapshots, versionnement d'événements.

---

## Ce que le projet couvre

| Thème | Patterns travaillés |
|---|---|
| Fondamentaux | producer/consumer, partitions, clés, consumer groups, offsets, rebalancing |
| Contrats | JSON Schema, Schema Registry, compatibilité BACKWARD/FORWARD, évolution de schéma |
| Fiabilité producer | `acks=all`, idempotent producer, `min.insync.replicas`, batching, ordre |
| Fiabilité consumer | idempotence applicative, ack manuel, retry topics, backoff, DLQ, poison message |
| Cohérence | outbox transactionnel, dual-write problem, transactions Kafka, exactly-once |
| Orchestration | saga chorégraphiée, compensation, timeout de saga, corrélation |
| Event sourcing | event store, rejeu, projections, snapshots, CQRS, versionnement d'événements |
| Stream processing | KStream/KTable, agrégations fenêtrées, joins, state stores, interactive queries |
| Tests | `@EmbeddedKafka`, Testcontainers, tests de contrat de schéma, tests de saga |
| Exploitation | consumer lag, métriques Micrometer, tracing distribué, compaction, tuning |

---

## Stack

| Composant | Version |
|---|---|
| Java | 25 (LTS) |
| Spring Boot | 4.2.0 |
| Spring Cloud / Stream | 2026.0.0 / 5.1.0 |
| Spring for Apache Kafka | 4.2.0 |
| Apache Kafka | 4.4.0 (**KRaft**) |
| Sérialisation | JSON Schema + Confluent Schema Registry 8.4.1 |
| Build | Maven 3.9.13, multi-module |
| Tests | Testcontainers 2.0.5, `@EmbeddedKafka` |
| Infra locale | Docker Compose + Kafbat Kafka UI v1.5.1 |
| Base | PostgreSQL 18 |

Détail complet et pièges de version : [CLAUDE.md](CLAUDE.md).

---

## Prérequis

- **JDK 25** — Microsoft OpenJDK 25.0.2+10. Le JDK du `PATH` est un 21, qui ne compile pas en `release 25` : positionner `JAVA_HOME` avant de builder.
- **Docker Desktop** (29.x) — nécessaire pour l'infra et les tests Testcontainers.
- Maven : utiliser le wrapper `./mvnw`, pas besoin d'installation globale.

Pointer Java 25 pour la session courante :

```powershell
$env:JAVA_HOME = "C:\Users\mouha\Documents\WorkspaceJAVA\1-MesJDKs\MicrosoftOpenJDKs\jdk-25.0.2+10"
```

Vérification :

```powershell
& "$env:JAVA_HOME\bin\java" -version; docker --version
```

---

## Démarrage

Lancer l'infra :

```powershell
docker compose -f infra/docker-compose.yml up -d
```

Vérifier que le broker répond :

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Construire :

```powershell
./mvnw clean verify
```

Lancer un service :

```powershell
./mvnw -pl order-service spring-boot:run
```

Passer une commande :

```powershell
Invoke-RestMethod -Uri http://localhost:8091/orders -Method Post -ContentType 'application/json' -Body '{"customerId":"c-1","items":[{"sku":"SKU-1","qty":2}]}'
```

Observer le flux dans Kafka UI : http://localhost:8080

---

## Expériences

Les mécanismes de Kafka s'apprennent en les faisant bouger. Chaque phase laisse des scripts
rejouables dans `infra/experiments/` :

```powershell
./infra/experiments/p1-t4-partitions.ps1 -Count 100
```

```powershell
./infra/experiments/p1-t5-rebalancing.ps1 -Instances 4
```

```powershell
./infra/experiments/p1-t6-offsets.ps1
```

```powershell
./infra/experiments/p1-t7-deux-modeles.ps1
```

Les garanties du producteur, elles, se mesurent en cassant quelque chose. Ces trois scripts
arrêtent ou mettent en pause le broker et purgent `shop.order.events.v1` ; ils demandent
`order-service` lancé avec `--spring.profiles.active=lab` :

```powershell
./infra/experiments/p3-t3-perte.ps1
```

```powershell
./infra/experiments/p3-t4-doublons.ps1
```

```powershell
./infra/experiments/p3-t5-debit.ps1 -Count 3000 -Repeat 3
```

Les contrats d'événements, eux, vivent dans le Schema Registry. Les publier est un acte
explicite, et leur évolution se teste avant d'être déployée :

```powershell
./infra/schemas/register-schemas.ps1
```

```powershell
./infra/schemas/p2-t5-evolution.ps1
```

Ce qu'elles montrent et les mesures relevées : `docs/patterns/`.

---

## Interfaces

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Schema Registry | http://localhost:8081 |
| order-service | http://localhost:8091 |
| payment-service | http://localhost:8092 |
| inventory-service | http://localhost:8093 |
| shipping-service | http://localhost:8094 |
| notification-service | http://localhost:8095 |
| — dont `GET /stats` | compteurs par partition de l'instance |
| analytics-service | http://localhost:8096 |

---

## Structure

```
common-events/        contrats d'événements + JSON Schemas
common-kafka/         serdes, error handlers, DLQ, idempotence, observabilité
order-service/        event sourcing + CQRS + outbox, démarre la saga
payment-service/      participant saga
inventory-service/    participant saga, exactly-once
shipping-service/     participant saga
notification-service/ consumer terminal idempotent
analytics-service/    Kafka Streams
infra/                docker-compose, topics, schemas, experiences
docs/                 TASKS.md, adr/, patterns/
```

---

## Avancement

Le découpage en phases, les tâches et leur état sont dans **[docs/TASKS.md](docs/TASKS.md)**. C'est la source de vérité de l'avancement.

Les fiches d'apprentissage produites au fil des phases sont dans `docs/patterns/`, les décisions d'architecture dans `docs/adr/`.

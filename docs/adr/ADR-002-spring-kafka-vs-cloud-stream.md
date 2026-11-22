# ADR-002 — spring-kafka ou Spring Cloud Stream, module par module

- **Statut** : accepté
- **Date** : 2026-11-22
- **Phase** : 1 (P1-T7)
- **Contexte mesuré** : `notification-service`, Spring Boot 4.2.0, spring-kafka 4.2.0,
  Spring Cloud Stream 5.1.0, Kafka 4.4.0

---

## Décision

| Module | Modèle | Raison |
|---|---|---|
| `order-service` | **spring-kafka** | outbox, transactions, event store : besoin d'accès direct au producer et aux offsets |
| `payment-service` | **spring-kafka** | retry étagé, DLT, idempotence manuelle (phase 4) |
| `inventory-service` | **spring-kafka** | exactly-once avec transactions Kafka (phase 7) |
| `shipping-service` | **spring-kafka** | participant de saga, même besoin de contrôle d'erreur |
| `notification-service` | **spring-kafka**, le binding fonctionnel restant disponible sous le profil `cloud-stream` | consumer terminal sans contrainte : c'est le seul module où les deux conviennent, donc le terrain de comparaison |
| `analytics-service` | **Spring Cloud Stream**, binder Kafka Streams | une topologie Streams est précisément ce que le binder gère mieux que du code à la main |

Règle générale retenue : **spring-kafka partout où la fiabilité est le sujet, Spring Cloud
Stream là où la topologie est le sujet.**

---

## Ce qui a été comparé

Le même cas écrit deux fois : lire `OrderPlaced` sur `shop.order.events.v1`, compter, logger.
Script : `infra/experiments/p1-t7-deux-modeles.ps1`.

```
spring-kafka : 5/5 consommés, 4,3 s jusqu'à UP, 180 lignes de log au démarrage
cloud-stream : 5/5 consommés, 3,7 s jusqu'à UP, 599 lignes de log au démarrage
```

Fonctionnellement équivalents. Les différences sont ailleurs.

### Le code

```java
// spring-kafka : le topic et Kafka sont nommés dans le code
@KafkaListener(topics = Topics.ORDER_EVENTS, groupId = "${spring.kafka.consumer.group-id}")
public void onOrderEvent(ConsumerRecord<String, OrderPlaced> record) { ... }
```

```java
// Spring Cloud Stream : un java.util.function.Consumer, rien de Kafka dans la signature
@Bean
public Consumer<Message<OrderPlaced>> onOrderPlaced() { ... }
```

Le second ne mentionne ni topic, ni groupe, ni broker : tout est en configuration. Le bean
pourrait être branché sur RabbitMQ sans être modifié. C'est l'argument du binder, et c'est
aussi sa limite — **ce qui est portable est l'intersection des brokers**, et l'intersection
ne contient ni les transactions Kafka, ni les state stores, ni le contrôle fin des offsets.

### La conversion du payload

Deux chaînes distinctes, qui se règlent à deux endroits différents :

- spring-kafka : un `Deserializer` Kafka (`JacksonJsonDeserializer`), configuré sous
  `spring.kafka.consumer` ;
- Cloud Stream : les `MessageConverter` de Spring Messaging, par `contentType`, le
  `Deserializer` Kafka étant court-circuité (`use-native-decoding: false`).

Conséquence pratique : un problème de type ne se diagnostique pas au même endroit, et les
garde-fous ne sont pas les mêmes (`spring.json.trusted.packages` ne s'applique qu'à la
première).

### Le vocabulaire

Le binder redéfinit des noms déjà connus : `group` pour `group.id`, `start-offset` pour
`auto-offset-reset`, `destination` pour le topic, et des bindings nommés
`<fonction>-in-<index>` sans rapport avec le nom du topic. Il faut donc apprendre deux
vocabulaires et savoir lequel gagne — coût réel sur un projet où l'on débogue du Kafka.

---

## Ce que la décision coûte

- **Plus de code dans les cinq services de la saga.** Les `@KafkaListener`, les error
  handlers et les conteneurs sont écrits à la main là où le binder en aurait caché une partie.
- **Deux modèles dans le même dépôt.** `analytics-service` ne ressemblera pas aux autres. Ce
  n'est pas une incohérence mais un choix à documenter — c'est l'objet de cet ADR.
- **La portabilité vers un autre broker est perdue** pour les cinq services. Assumé : le
  projet porte sur Kafka, et les patterns travaillés (transactions, EOS, event store,
  compaction) ne sont pas portables de toute façon.

---

## Pourquoi pas l'inverse

Tout en Spring Cloud Stream aurait donné un code plus court dès la phase 1. Mais :

- l'objectif est d'**apprendre les mécanismes Kafka**, et le binder les cache. Un
  `DeadLetterPublishingRecoverer` écrit à la main enseigne ce qu'une propriété
  `enableDlq: true` escamote (CLAUDE.md §6, règle 7) ;
- les phases 4 à 8 ont besoin de ce que le binder n'expose pas ou expose indirectement :
  retry étagé sur topics distincts, transactions producteur-consumer, lecture de l'event store
  depuis l'offset 0, commit manuel ;
- le modèle fonctionnel reste disponible dans `notification-service` : la comparaison restera
  rejouable à chaque phase, et la décision révisable.

---

## Conséquences

- `analytics-service` utilisera `spring-cloud-stream-binder-kafka-streams` (phase 9).
- Le profil `cloud-stream` de `notification-service` est maintenu en état de marche : toute
  phase qui modifie la consommation doit le vérifier (`p1-t7-deux-modeles.ps1`).
- Les deux consumers ne tournent jamais ensemble : groupes distincts, chaque événement serait
  compté deux fois. `@Profile` les rend exclusifs.

Voir aussi : [ADR-001 — choix de la stack](ADR-001-choix-stack.md) ·
[fiche 01 — partitions, clés, groupes](../patterns/01-partitions-cles-groupes.md)

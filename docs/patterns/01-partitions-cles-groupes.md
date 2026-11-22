# 01 — Partitions, clés, groupes, offsets

> Phase 1. Mesures relevées le 2026-11-22 sur l'infra locale (Kafka 4.4.0, topic
> `shop.order.events.v1`, 3 partitions, réplication 1).

---

## Le problème

Un topic doit tenir deux promesses qui se contredisent :

1. **l'ordre** — `OrderPlaced` doit être traité avant `OrderCancelled` de la même commande ;
2. **le débit** — plusieurs consumers doivent travailler en parallèle.

L'ordre total impose un seul consumer. Le parallélisme impose d'abandonner l'ordre total.
Kafka ne tranche pas à votre place : il découpe le topic en **partitions** et ne garantit
l'ordre qu'à l'intérieur de chacune. C'est vous qui décidez ce qui doit rester ordonné, en
choisissant la **clé**.

---

## Le mécanisme

```
   producer                      topic shop.order.events.v1
  ┌────────┐     hash(clé) % 3   ┌────────────────────────────┐
  │ clé =  │ ──────────────────▶ │ partition 0 : [0][1][2]... │ ──┐
  │ orderId│                     │ partition 1 : [0][1]...    │ ──┼─▶ un groupe
  └────────┘                     │ partition 2 : [0][1][2]... │ ──┘   de consumers
                                 └────────────────────────────┘
```

Quatre notions, souvent confondues :

| Notion | Ce que c'est | Ce qui en dépend |
|---|---|---|
| **Partition** | un log ordonné, append-only | l'unité d'ordre **et** de parallélisme |
| **Clé** | les octets qui choisissent la partition | quels messages restent ordonnés entre eux |
| **Groupe** | un ensemble de consumers qui se partagent les partitions | le partage du travail |
| **Offset** | la position d'un groupe dans une partition | ce qu'un groupe a déjà lu — rien d'autre |

L'offset appartient au **groupe**, pas au topic. Lire ne consomme rien : les messages restent
jusqu'à expiration de la rétention, et n'importe quel autre groupe peut les relire.

---

## Le code clé

La clé est déclarée par l'événement lui-même, pas par l'appelant :

```java
public interface DomainEvent {
    EventEnvelope envelope();
    String partitionKey();   // l'événement sait sur quoi il doit être ordonné
}
```

```java
// OrderPlaced : tout ce qui concerne une commande part sur la même partition
@Override
public String partitionKey() {
    return orderId;
}
```

Si le choix de la clé était laissé au code appelant, rien n'empêcherait de publier deux
événements de la même commande sous deux clés différentes — ils partiraient sur deux
partitions et pourraient être traités dans le désordre. Le mettre dans l'événement rend
l'erreur impossible.

Côté publication, partition laissée à `null` : c'est le partitioner qui tranche.

```java
ProducerRecord<String, Object> record =
        new ProducerRecord<>(topic, null, event.partitionKey(), event);
```

Côté consommation, le groupe vient de la configuration, jamais d'une constante : c'est ce qui
permet de lancer plusieurs instances dans le même groupe (partage) ou dans des groupes
distincts (diffusion) sans recompiler.

```java
@KafkaListener(topics = Topics.ORDER_EVENTS, groupId = "${spring.kafka.consumer.group-id}")
```

---

## Ce que les expériences ont montré

### 1. Des clés différentes se répartissent à peu près, pas exactement

100 commandes, 100 `orderId` distincts, 3 partitions
(`infra/experiments/p1-t4-partitions.ps1`) :

| Partition | Messages | Part |
|---|---|---|
| 0 | 33 | 33 % |
| 1 | 30 | 30 % |
| 2 | 37 | 37 % |

Le hachage n'équilibre pas à l'unité sur de petits volumes. Attendre 33/33/33 est une erreur
de lecture : l'écart se resserre avec le volume, et une clé à faible cardinalité (le pays du
client, par exemple) crée un déséquilibre permanent qu'aucun volume ne corrige.

### 2. La même clé tombe toujours sur la même partition

| Clé | Partition |
|---|---|
| `order-A` (5 messages) | 2, 2, 2, 2, 2 |
| `order-B` | 0 |
| `order-C` | 0 |
| `order-D` | 1 |

Deux clés différentes peuvent partager une partition (`order-B` et `order-C`) : c'est sans
conséquence, elles sont simplement ordonnées entre elles sans l'avoir demandé.

**Une clé est une suite d'octets, pas un texte.** Le script produisait au départ les clés par
un pipe PowerShell, qui préfixe le flux d'un BOM UTF-8 : la première clé devenait
`﻿order-A` et partait sur la partition 1 au lieu de la 2 — pour un affichage identique à
l'écran. Deux clés visuellement semblables mais encodées différemment ne sont pas la même
clé, et l'ordre entre elles est perdu.

### 3. Le nombre de partitions est le plafond du parallélisme

4 instances de `notification-service` dans le même groupe, topic à 3 partitions
(`infra/experiments/p1-t5-rebalancing.ps1`) :

| Instances dans le groupe | Assignation |
|---|---|
| 1 | partitions 0, 1, 2 |
| 2 | 2 partitions + 1 |
| 3 | 1 chacune |
| 4 | 1 chacune pour trois, **une instance sans partition** |

L'instance en trop ne traite rien : `port 8195 : 0 événement, aucune partition assignée`.
Elle n'est pas inutile pour autant — elle reprend le travail si une autre tombe. Et ce n'est
pas la dernière démarrée qui est écartée : l'assignation ne suit pas l'ordre d'arrivée.

Conséquence de dimensionnement : ajouter des consumers au-delà du nombre de partitions
n'augmente rien. Le nombre de partitions se décide à la création du topic, et on ne peut
qu'en ajouter — jamais en retirer. Ajouter des partitions redistribue les clés, donc casse
l'ordre des messages déjà publiés par rapport aux nouveaux (sujet de P12-T1).

### 4. Rejouer, c'est déplacer un offset — rien d'autre

`infra/experiments/p1-t6-offsets.ps1` :

```
--to-earliest  -> NEW-OFFSET 0 sur les 3 partitions, LAG = 86 / 95 / 91
redémarrage    -> 272 événements retraités (p0=86 p1=95 p2=91)
--to-datetime  -> offsets 31 / 39 / 31, rejeu partiel de 171 événements
```

Aucun message n'a été republié. C'est la propriété la plus utile de Kafka et la plus facile à
oublier : un consumer bogué peut être corrigé puis **relancé sur le passé**, ce qu'aucune file
de messages classique ne permet.

Deux refus instructifs :

- **on ne déplace pas les offsets d'un groupe actif.**
  `Error: Assignments can only be reset if the group 'notification-service' is inactive, but
  the current state is Stable.` Il faut arrêter tous les membres.
- **un process tué de force reste membre du groupe.** Mesure : **47 s** avant que le groupe
  passe à `Empty`. Un arrêt brutal n'envoie pas de `LeaveGroup`, donc le coordinateur attend
  l'expiration de `session.timeout.ms` (45 s par défaut). C'est aussi le délai avant que les
  partitions d'un consumer qui crashe soient réattribuées — à la différence d'un arrêt propre,
  immédiat.

---

## Comment le casser, pour voir l'échec

| Expérience | Conséquence attendue |
|---|---|
| Publier avec `null` comme clé | Répartition round-robin ; deux événements de la même commande peuvent arriver dans le désordre. Interdit sur un topic métier (CLAUDE.md §2). |
| Mettre `customerId` en clé | L'ordre est garanti par client, et toutes les commandes d'un gros client sérialisent sur une seule partition : un *hot partition*. |
| Lancer 2 instances avec `CONSUMER_GROUP` différent | Chacune reçoit **tout**. C'est de la diffusion, pas du partage — utile pour un nouveau read-model, désastreux si on croyait répartir la charge. |
| `--reset-offsets --to-earliest` sans idempotence | Les 272 événements sont retraités, effets de bord compris. Motivation directe de la table `processed_event` (phase 4). |
| Publier un message illisible dans le topic | Le `JacksonJsonDeserializer` nu échoue avant le listener, l'offset n'est jamais commité, la partition est bloquée. C'est le *poison message* (phase 4). |

---

## Comment vérifier

```powershell
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:19092 --topic shop.order.events.v1
```

```powershell
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group notification-service
```

```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic shop.order.events.v1 --from-beginning --property print.key=true --property print.partition=true --timeout-ms 8000
```

Les trois expériences, rejouables :

```powershell
./infra/experiments/p1-t4-partitions.ps1 -Count 100
```

```powershell
./infra/experiments/p1-t5-rebalancing.ps1 -Instances 4
```

```powershell
./infra/experiments/p1-t6-offsets.ps1
```

---

## À retenir avant la phase 2

- La clé décide de l'ordre. Choisir la clé, c'est choisir ce qui doit rester séquentiel —
  c'est une décision métier, pas un détail technique.
- Le nombre de partitions plafonne le parallélisme d'un groupe et se décide à l'avance.
- L'offset appartient au groupe. Le rejeu est gratuit et ne touche pas les messages.
- Kafka livre **au moins une fois**. Sans idempotence consumer, tout rejeu duplique les
  effets de bord. C'est la dette de la phase 1, payée en phase 4.
- Un consumer qui crashe laisse ses partitions bloquées le temps de `session.timeout.ms`.

Pièges voisins : [00 — Pourquoi l'event-driven](00-pourquoi-event-driven.md) ·
décision sur le modèle de programmation : [ADR-002](../adr/ADR-002-spring-kafka-vs-cloud-stream.md)

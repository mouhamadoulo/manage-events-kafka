# 03 — Ce qu'un `send()` garantit

> Phase 3. Mesures relevées le 2026-11-22 sur l'infra locale (Kafka 4.4.0, **un seul broker**,
> réplication 1, client `kafka-clients` 4.4.0).

---

## Le problème

```java
kafkaTemplate.send(topic, key, event);
```

Cette ligne compile, passe les tests, et ne garantit **rien**. Elle rend la main quand le
record est dans un tampon mémoire, pas quand il est écrit. Trois questions restent ouvertes,
et chacune a une réponse différente selon la configuration :

1. le record est-il arrivé ?
2. le producteur le **sait**-il ?
3. est-il arrivé **une seule fois** ?

La phase 3 répond en mesurant, parce que ces garanties ne se lisent pas dans le code.

---

## Le mécanisme

```
   send(record)
        │
        ▼
  ┌───────────────┐   un lot par partition, ferme quand
  │ buffer 32 Mo  │   batch.size est atteint OU linger.ms écoulé
  └──────┬────────┘
         │  max.in.flight.requests.per.connection lots en vol
         ▼
    ┌─────────┐   acks=0   : le future réussit dès l'écriture sur la socket
    │ broker  │   acks=1   : le leader a écrit
    └─────────┘   acks=all : toutes les replicas synchrones ont écrit
         │
         ▼  accusé (ou pas)
   le future se termine — et c'est au code de le regarder
```

Quatre horloges, souvent confondues :

| Réglage | Ce qu'il borne |
|---|---|
| `max.block.ms` | le temps que `send()` passe **à bloquer** quand le buffer est plein |
| `request.timeout.ms` | l'attente d'une réponse pour **une** tentative |
| `delivery.timeout.ms` | le budget **total** d'un `send()`, réessais compris |
| `linger.ms` | l'attente volontaire pour **remplir un lot** |

---

## Mesure 1 — `acks=0` perd sans le dire

Protocole : rafale de 200 000 messages, `docker kill kafka` au bout de 4 s, redémarrage, puis
comparaison entre ce que le producteur déclare et ce que le topic contient
(`infra/experiments/p3-t3-perte.ps1`).

| `acks` | déclarés envoyés | échecs | réellement dans le topic | écart |
|---|---|---|---|---|
| `0` | 7 481 | 2 957 | 6 231 | **1 250 perdus** |
| `all` | 62 769 | 3 978 | 62 769 | **0** |

Avec `acks=0`, **1 250 records que le producteur considère envoyés ne sont nulle part**. Aucune
exception, aucun log d'erreur, aucune métrique en défaut. C'est la pire forme de panne : celle
qui ne laisse pas de trace.

Avec `acks=all`, les deux nombres sont égaux. Les 3 978 envois que le broker n'a pas confirmés
sont comptés en échecs — donc visibles, donc rattrapables.

**Ce que la mesure ne dit pas** : sur ce labo mono-nœud, `acks=all` ne protège pas d'une perte
côté broker, puisqu'il n'y a qu'une replica. Ce qu'il apporte ici, c'est que le producteur
**sait** ce qui a réussi. La vraie durabilité vient du couple `replication.factor >= 3` /
`min.insync.replicas = 2` (phase 12).

> Un échec connu est un problème d'ingénierie. Une perte silencieuse est un problème de
> confiance : plus rien de ce que le système affirme n'est vérifiable.

---

## Mesure 2 — sans idempotence, le réessai duplique

Protocole : `request.timeout.ms = 5 ms`, en dessous du temps de réponse normal du broker.
Chaque requête est donc déclarée expirée alors que le broker l'a traitée, et le producteur
réessaye (`infra/experiments/p3-t4-doublons.ps1`).

| Configuration | déclarés | records dans le topic | `eventId` distincts | doublons |
|---|---|---|---|---|
| `enable.idempotence=false` | 20 000 | **47 156** | 20 000 | **27 156** (+135,8 %) |
| `enable.idempotence=true` | 20 000 | 20 000 | 20 000 | **0** |

Les 20 000 `eventId` distincts dans les deux cas prouvent qu'il s'agit bien des **mêmes
records réécrits**, pas d'envois supplémentaires.

Le mécanisme : avec l'idempotence, le producteur numérote ses records (*producer id* +
numéro de séquence par partition). Le broker garde les dernières séquences vues et rejette
un doublon. Sans cette numérotation, deux requêtes identiques sont deux écritures.

**Ce que l'idempotence du producteur ne couvre pas** — et c'est le plus important :

- elle vaut pour **un même producteur, dans une même session**. Un service qui republie après
  un redémarrage duplique malgré elle ;
- elle ne dit rien d'un **consumer** qui retraite un message (phase 4) ;
- elle ne couvre pas l'écriture atomique de plusieurs topics (phase 7).

L'idempotence producteur est nécessaire, jamais suffisante.

### Trois impasses avant d'y arriver

**`docker pause` ne duplique pas.** Geler le broker l'empêche aussi bien d'**écrire** que
d'**accuser** : il reprend où il en était et accuse normalement. Le doublon exige que
l'écriture réussisse **et** que l'accusé soit perdu — c'est un timeout *client* qu'il faut, pas
une panne serveur.

**`Start-Job` met une à deux secondes à démarrer.** Avec une rafale d'une seconde, le broker
était tué avant le premier envoi : l'expérience mesurait le démarrage de PowerShell. Il a fallu
une rafale de 200 000 messages pour qu'elle soit interruptible.

**Un buffer plein fait bloquer `send()` pendant `max.block.ms`** (60 s par défaut) avant
d'échouer. Sur 20 000 messages, cela prend des heures sans rien apprendre de plus que les
204 premiers timeouts. L'outil de rafale s'arrête donc au premier échec synchrone — ce qu'une
application réelle fait aussi : elle cesse d'accepter.

---

## Mesure 3 — le tuning réseau ne mesure rien ici

3 000 messages par scénario, **3 essais**, médiane, après échauffement
(`infra/experiments/p3-t5-debit.ps1`) :

| Scénario | médiane | essais | écart |
|---|---|---|---|
| défaut (`linger=0`, sans compression) | 20 408 msg/s | 19 736 / 21 428 / 20 408 | x1 |
| `linger=5ms`, `batch=32k` | 17 341 msg/s | 23 255 / 14 423 / 17 341 | x0,85 |
| `linger=20ms`, `batch=256k` | 16 759 msg/s | 14 563 / 19 354 / 16 759 | x0,82 |
| idem + `lz4` | 16 483 msg/s | 16 483 / 11 583 / 23 255 | x0,81 |
| idem + `zstd` | 23 255 msg/s | 26 548 / 23 255 / 22 058 | x1,14 |
| idem, **validation JSON Schema coupée** | 30 303 msg/s | 30 303 / 28 846 / 33 333 | **x1,48** |

La conclusion est l'inverse de celle attendue : **la dispersion entre essais d'un même scénario
(11 583 à 26 548 msg/s) dépasse l'écart entre scénarios.** Sur ce labo — boucle locale, un
broker, petits messages — `linger.ms`, `batch.size` et `compression.type` ne sont pas
mesurables. Les classer serait inventer un résultat.

Le seul facteur qui déplace la médiane de façon lisible est la **validation du schéma**, faite
sur le thread appelant à chaque envoi. Autrement dit : le facteur limitant n'était pas le
réseau, et régler `linger.ms` ici n'aurait rien donné.

Deux précautions ont été nécessaires pour arriver à ce constat, et elles comptent autant que
le résultat :

- **un échauffement.** Sans lui, le premier scénario payait le JIT, le chargement des classes
  et la résolution du schéma : 773 msg/s au lieu de 20 408. Comme c'était le scénario de
  référence, **tous les écarts étaient faux** — et dans le sens flatteur, puisque chaque
  réglage suivant paraissait un progrès (jusqu'à x6,7) ;
- **des répétitions.** Une seule mesure donnait `linger=5ms` à x4,28 puis à x0,41 selon
  l'essai.

---

## Le code clé

Le piège de la phase n'est pas une mauvaise valeur de configuration, c'est une ligne qui ne
regarde pas son résultat :

```java
kafkaTemplate.send(record);   // compile, passe les tests, perd en silence
```

Il n'y a **rien à lire** dans cette ligne : l'oubli est invisible à la relecture. D'où un
`publishAsync` qui impose le callback :

```java
return send(topic, event).whenComplete((result, failure) -> {
    if (failure != null) {
        log.error("echec de publication asynchrone {} eventId={} : {}",
                envelope.eventType(), envelope.eventId(), failure.toString());
    }
});
```

Et côté synchrone, une exception déballée puis traduite, parce qu'un échec de publication est
un refus de prendre la commande, pas un bug du serveur :

```java
} catch (CompletionException e) {
    Throwable cause = e.getCause() == null ? e : e.getCause();
    log.error("echec de publication {} eventId={} : {}", ..., cause.toString());
    throw new EventPublicationFailedException(..., cause);
}
```

Le contrôleur répond **503**, avec un message générique : celui du client Kafka nomme l'hôte
du broker, celui du registre, le sujet et les timeouts. Le client HTTP n'en a pas besoin pour
réessayer.

---

## Comment le casser, pour voir l'échec

| Expérience | Conséquence attendue |
|---|---|
| `send()` sans rien faire du future | Les échecs disparaissent. Aucune trace, nulle part. C'est le défaut de fabrication le plus courant. |
| `acks=0` | Mesuré : 1 250 records sur 7 481 perdus sans un mot. |
| `enable.idempotence=false` + timeout serré | Mesuré : +135,8 % de records écrits. |
| `max.in.flight=5` **sans** idempotence | Un lot réessayé peut passer **après** le lot suivant : l'ordre de la partition est cassé, alors que c'est la seule garantie d'ordre de Kafka. |
| `delivery.timeout.ms` < `linger.ms` + `request.timeout.ms` | Le producteur refuse de démarrer. Arrive vite quand on bricole `linger.ms`. |
| `max.block.ms` par défaut (60 s) + broker mort | L'application paraît gelée. Elle fait exactement ce qu'on lui a demandé. |
| Mesurer un réglage sans échauffement ni répétition | On conclut l'inverse de la réalité, avec des chiffres à l'appui. |

---

## Comment vérifier

Démarrer `order-service` avec l'outil de rafale (profil `lab` uniquement) :

```powershell
./mvnw -pl order-service spring-boot:run -Dspring-boot.run.profiles=lab
```

```powershell
./infra/experiments/p3-t3-perte.ps1
```

```powershell
./infra/experiments/p3-t4-doublons.ps1
```

```powershell
./infra/experiments/p3-t5-debit.ps1 -Count 3000 -Repeat 3
```

Compter ce que le topic contient vraiment :

```powershell
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:19092 --topic shop.order.events.v1
```

---

## À retenir avant la phase 4

- `send()` ne garantit rien tant qu'on ne regarde pas son résultat. Le reste de la
  configuration ne sert à rien sans cela.
- `acks=0` est un choix de perte silencieuse. `acks=all` ne rend pas durable à lui seul : il
  rend **honnête**.
- L'idempotence du producteur supprime les doublons d'un **réessai**, dans une **session**.
  Elle ne remplace ni l'idempotence consumer, ni les transactions.
- `max.in.flight > 1` ne préserve l'ordre que **grâce** à l'idempotence.
- Avant de régler `linger.ms` ou la compression : mesurer, avec échauffement et répétitions.
  Sinon on optimise un facteur qui n'est pas le facteur limitant.
- Rien de tout cela ne résout le **dual-write** : si l'état est en base et l'événement dans
  Kafka, aucune configuration de producteur ne les rend atomiques. C'est l'objet de la
  phase 5 (outbox).

Fiches voisines : [01 — partitions, clés, groupes](01-partitions-cles-groupes.md) ·
[02 — Schema Registry et évolution](02-schema-registry-evolution.md)

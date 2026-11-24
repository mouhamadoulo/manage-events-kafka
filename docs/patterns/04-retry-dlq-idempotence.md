# 04 — Un consumer qui survit à un message empoisonné et à un rejeu

> Phase 4. Code : `common-kafka/src/main/java/com/formation/eda/kafka/reliability/`,
> `notification-service`. Expérience : `infra/experiments/p4-t6-poison.ps1`.
> Design : [docs/superpowers/specs/2026-11-23-phase-4-fiabilite-consumer-design.md](../superpowers/specs/2026-11-23-phase-4-fiabilite-consumer-design.md).

---

## Le problème

La phase 3 a rendu le **producteur** fiable. Le consumer, lui, était resté nu depuis P1-T3, et
pour deux raisons volontaires :

1. **Un seul message mal formé bloquait une partition pour toujours.** La désérialisation
   échoue *avant* le listener. L'offset n'est donc jamais commité, le même message est repollé
   indéfiniment, et tout ce qui est publié derrière lui attend. Mesuré : **10 messages sur 20
   traités, lag bloqué à 11, offset figé à 6641** — celui du message empoisonné.

2. **Kafka livre au moins une fois.** Un rebalancing, un redémarrage, un reset d'offsets
   (272 événements retraités en P1-T6) ou un rejeu depuis la DLT font repasser les mêmes
   événements. Sans registre, chaque passage produit un nouvel effet métier.

Ce n'est pas un détail d'exploitation : la saga de la phase 6 est une **chaîne** de consumers.
Un consumer qui bloque sa partition bloque la saga.

---

## Le mécanisme

```
shop.order.events.v1
  │
  ├─ ErrorHandlingDeserializer
  │     un échec de désérialisation ne lève plus : il produit un record à valeur NULLE
  │     portant l'exception en header. Le record atteint donc le conteneur.
  │
  ├─ IdempotentRecordInterceptor
  │     lit l'eventId dans un HEADER (pas dans le payload : un record illisible n'en a pas)
  │     INSERT ... ON CONFLICT DO NOTHING
  │       1 ligne → on traite
  │       0 ligne → intercept() rend null : le listener n'est PAS appelé, l'offset est commité
  │
  ├─ OrderNotificationHandler            ← le même code pour les trois listeners
  │     échec → DefaultErrorHandler
  │               non-retryable → 0 retry, DLT directe
  │               retryable     → 2 retries bloquants (200 ms, 400 ms + jitter)
  │
  └─ DeadLetterPublishingRecoverer + résolveur de destination
        topic → .retry-1 → .retry-2 → .dlt

.retry-1 (dû à T+5 s) / .retry-2 (dû à T+30 s)
    listeners dédiés, qui appellent le MÊME handler.
    Pas encore dû → NotYetDueException → la partition est mise en pause.

.dlt  aucun listener. Lue à la demande par GET /admin/dlt, rejouée par POST /admin/dlt/replay.
```

Trois décisions portent tout le reste.

### 1. L'`eventId` vit dans un header, pas seulement dans le payload

Un header se lit **sans désérialiser**. C'est ce qui permet à l'intercepteur de fonctionner, et
c'est indispensable pour un record dont la désérialisation a échoué : il n'a pas de payload du
tout.

### 2. La clé d'idempotence est `(event_id, consumer_group)`

Pas `event_id` seul. En phase 6, les 5 services de la saga liront les mêmes topics dans **5
groupes différents**, et chacun doit traiter l'événement une fois. Avec une clé sur `event_id`
seul, le premier service le marquerait pour tous les autres.

`source_topic` est journalisé mais **hors de la clé** : un record venu de `.retry-1` est le
*même* événement. S'il entrait dans la clé, chaque étage de retry rejouerait l'effet métier —
le retry produirait les doublons que la table existe pour empêcher.

### 3. Le différé d'un étage passe par une **exception**

C'est la décision la plus contre-intuitive de la phase, et elle vient d'un bug réel de ce
projet. Voir [Le piège central](#le-piège-central).

---

## Le piège central

> **La marque d'idempotence est posée *avant* le traitement. Tout chemin de sortie du listener
> doit donc lever.**

Pourquoi marquer avant ? Les trois réponses possibles, et pourquoi celle-là :

| Réponse | Verdict |
|---|---|
| Marquer **après** l'effet métier | Un crash entre les deux rejoue l'effet. Pire pour un effet non idempotent (un mail). |
| Marquer **dans la même transaction** que l'effet | La bonne réponse quand l'effet est dans la même base. C'est la phase 5. Inapplicable ici : l'effet de `notification-service` est un compteur en mémoire. |
| Marquer avant, **effacer la marque sur échec** (`RecordInterceptor.failure`) | Retenue. |

Et voilà la conséquence, qui n'était pas prévue au design. `failure()` n'est appelé **que sur
exception**. La première version du différé faisait `consumer.seek()` + `consumer.pause()` puis
un **retour normal**. Résultat : la marque restait en place sur un travail jamais fait, et au
réveil de la partition l'événement était classé doublon et sauté. **Tous les messages en
attente étaient perdus, sans une ligne de log.**

D'où `NotYetDueException`. En levant, trois choses tombent juste d'un coup :

- `RecordInterceptor.failure()` part, donc la marque est retirée ;
- l'offset n'est **pas** commité, donc le record sera bien redélivré ;
- le gestionnaire d'erreur prend la main, et c'est lui qui sait mettre une partition en pause
  sans bloquer de thread.

Limite assumée : un crash brutal entre le marquage et l'échec laisse une marque orpheline, donc
un message sauté. La réponse correcte est d'écrire la marque dans la même transaction que
l'effet — c'est la raison d'être de la phase 5.

Un second garde-fou a été nécessaire, trouvé par revue de sécurité : `failure()` ne doit défaire
**que ce que `intercept()` a posé**. Sinon un record passé *sans* déduplication — payload nul,
header illisible — mais portant un `eventId` déjà traité effaçait la marque d'un travail déjà
fait. Une copie illisible d'un événement suffisait à le faire rejouer. Suivi par `ThreadLocal`,
nettoyé dans `afterRecord()`.

---

## Mesure 1 — le message empoisonné

`infra/experiments/p4-t6-poison.ps1`. Tout sur **une seule partition** (même clé partout) :
10 événements valides, un message brut sans octet magique, 10 valides de plus.

| | protégé | témoin (sans `ErrorHandlingDeserializer`) |
|---|---|---|
| valides publiés | 20 | 20 |
| **traités** | **20** | **10** |
| messages en `.dlt` | 1 | 0 |
| messages en `.retry-1` | 0 | 0 |
| lag final | 0 | **11** |
| offset du groupe | 6631 (= fin du log) | **6641, figé** |

Deux choses à lire dans ce tableau.

**Le témoin** est la faille de la phase 1 : les 10 messages publiés après le poison ne sont
jamais consommés. Pas « plus tard » — jamais.

**`.retry-1` reste à 0 côté protégé.** Un échec de désérialisation est classé
**non-retryable** : il part directement en DLT. Un message qui ne se décode pas ne se décodera
pas mieux dans 5 secondes, et le réessayer coûterait 35 secondes d'attente avant de signaler le
vrai problème.

---

## Mesure 2 — les étages de retry et leurs délais

`RetryChainTest`, délais ramenés à 400 ms / 900 ms (avec les 5 s / 30 s de production le test
durerait 40 s ; le rapport entre étages est conservé). Horodatages relevés dans les logs :

```
20:15:46.177  échec sur shop.order.events.v1          (panne injectée)
20:15:46.752  retraité depuis .retry-1                → 575 ms après, délai d'étage 400 ms
20:15:47.305  retraité depuis .retry-2                → dû à origine+900 ms, traité à +1128 ms
```

L'écart entre le délai configuré et le délai observé est le temps de republication plus un
cycle de `poll()`. Il est **systématiquement positif** : un étage ne traite jamais en avance.

Point qui compte : l'échéance se compte depuis **`x-origin-timestamp`**, l'instant de
publication initiale — pas depuis l'arrivée dans l'étage. Un délai compté à l'arrivée
repartirait de zéro à chaque republication, et un message pourrait attendre indéfiniment en
traversant les étages.

---

## Mesure 3 — ce que le rejeu garantit, exactement

La formulation fausse, celle du premier design : « un rejeu est sauté par l'idempotence ».
C'est faux, et si c'était vrai le rejeu ne servirait à rien : un message arrivé en DLT après un
échec a vu sa marque **effacée**.

La propriété vraie : **rejouer deux fois ne produit qu'un effet.** Vérifié en réel, sur l'infra
Docker :

```
POST /lab/producer/one?failFatal=true   → eventId 2e5eedca-…
GET  /admin/dlt?limit=3                 → offset=23  eventId=2e5eedca-…  étage=0
POST /admin/dlt/replay  {offsets:[23]}  → {"republished":1}   compteur métier : 0 → 1
POST /admin/dlt/replay  {offsets:[23]}  → {"republished":1}   compteur métier : 1 → 1
```

Les **deux** appels republient. Ce n'est pas l'endpoint qui déduplique, c'est le consumer, par
`processed_event` — comme pour n'importe quelle autre livraison. L'endpoint ne tient aucun
historique de ses propres appels, et c'est voulu : un rejeu après un correctif de code doit
retraiter.

`étage=0` dans la sortie : le message est allé **directement** du topic métier à la DLT, sans
traverser d'étage. C'est le classement non-retryable qui le dit.

---

## Le code clé

**L'intercepteur** — rendre `null` depuis `intercept()` fait que spring-kafka n'appelle pas le
listener **et commite quand même l'offset**. C'est exactement le comportement voulu pour un
doublon : ne rien faire, et avancer.

```java
if (store.markProcessed(eventId, consumerGroup, record.topic())) {
    markedByThisThread.set(eventId);
    return record;
}
return null;                       // doublon : listener non appelé, offset commité
```

**Le store** — pas de `SELECT` préalable. `SELECT` puis `INSERT` est une condition de course :
deux instances du même groupe peuvent passer le test en même temps et traiter l'événement deux
fois, ce qui est précisément le bug que la table existe pour empêcher. La contrainte d'unicité
est le seul arbitre.

```sql
insert into processed_event (event_id, consumer_group, source_topic)
values (?, ?, ?)
on conflict (event_id, consumer_group) do nothing
```

**Le différé** — `setBackOffFunction` reçoit le record *et l'exception*. C'est la seule façon
d'attendre la durée restante propre à chaque record ; sans elle, il faudrait un pas fixe et une
re-vérification à chaque réveil.

```java
handler.setBackOffFunction((record, exception) ->
        rootNotYetDue(exception) instanceof NotYetDueException notYetDue
                ? new FixedBackOff(notYetDue.remainingMillis(), 3L)
                : straightToNextStage);
```

**La DLT** — deux `KafkaTemplate`, choisis par type de payload. Un échec de désérialisation n'a
pas d'objet à republier, seulement les octets d'origine : les passer au sérialiseur JSON Schema
échouerait une seconde fois, à l'intérieur du mécanisme qui devait sauver le message.

```java
templates.put(byte[].class, byteArrayDltTemplate);
templates.put(Object.class, eventDltTemplate);
```

---

## Ce que `@RetryableTopic` aurait fait

L'annotation de spring-kafka câble exactement les mêmes briques :
`ContainerPartitionPausingBackOffManager`, `KafkaBackoffAwareMessageListenerAdapter`,
`DeadLetterPublishingRecovererFactory`. **En production, c'est ce qu'il faut utiliser.** Ici on
l'a assemblé à la main pour trois raisons, dont une purement pratique :

1. le mécanisme devient lisible — on peut pointer la ligne qui fait le retry, celle qui route
   vers la DLT, celle qui déduplique ;
2. le piège du §« piège central » ne se voit *que* si on écrit le différé soi-même ;
3. **le nommage ne correspond pas.** `javap -c` sur `DestinationTopicPropertiesFactory` montre
   que `joinWithRetrySuffix` colle le séparateur `-` **en dur** et indexe à partir de 0, avec
   `DEFAULT_RETRY_SUFFIX = "-retry"`. Avec un suffixe `.retry`, l'annotation produit
   `shop.order.events.v1.retry-0` et `.retry-1` — décalés d'un cran par rapport aux 12 topics
   créés en P0-T6 et aux sujets enregistrés en P2-T3.

---

## Comment le casser, pour voir l'échec

| Ce qu'on casse | Ce qui se passe | Comment on le voit |
|---|---|---|
| Retirer `ErrorHandlingDeserializer` (`VALUE_DESERIALIZER=io.confluent…KafkaJsonSchemaDeserializer`) | Un message hors format bloque la partition **pour toujours** | `p4-t6-poison.ps1 -Temoin` : lag 11 qui ne redescend pas, offset figé |
| Remplacer `throw new NotYetDueException(...)` par un `return` | Tous les messages en attente de retry sont **perdus en silence** | `RetryChainTest.anUndueRecordIsProcessedAfterThePauseExactlyOnce` : compteur à 0 au lieu de 1 |
| Dormir dans le listener au lieu de mettre la partition en pause | Au-delà de `max.poll.interval.ms` (défaut 5 min), l'instance sort du groupe → rebalancing en boucle | le groupe ne se stabilise jamais (`--describe --state`) |
| Classer `NotYetDueException` comme non-retryable | Un message arrivé **trop tôt** part en DLT au premier passage | `.dlt` se remplit de messages parfaitement valides |
| Mettre `source_topic` dans la clé primaire | Chaque étage de retry **rejoue l'effet métier** | `JdbcProcessedEventStoreIT.theSourceTopicIsNotPartOfTheEventIdentity` |
| Déclarer un seul `KafkaTemplate` | Le recoverer utilise le même pour les deux types de payload → `SerializationException: Can't convert value of class OrderPlaced to class ByteArraySerializer`, **aucun message n'atteint la DLT** | `RetryChainTest` (invisible en test unitaire) |
| Retirer le header `x-origin-topic` d'un message en DLT | Le rejeu republierait sur `.dlt` elle-même : **boucle infinie** alimentée par l'opérateur | `DltReplayTest.aRecordWithoutOriginTopicIsRejected` → `rejected` |
| Déclarer `flyway-core` sans `spring-boot-starter-flyway` | Aucune migration appliquée, **en silence**. Chaque événement part en DLT sur `BadSqlGrammarException` | `NotificationServiceApplicationTest.bootFlywayAutoConfigurationIsOnTheClasspath` |

---

## Comment vérifier

```powershell
$env:JAVA_HOME = "C:\Users\mouha\Documents\WorkspaceJAVA\1-MesJDKs\MicrosoftOpenJDKs\jdk-25.0.2+10"
```

```powershell
./mvnw clean verify
```

→ BUILD SUCCESS, 9 modules, **109 tests** : `common-events` 13, `common-kafka` 64,
`order-service` 4, `notification-service` 24, 1 test de contexte par service restant.

```powershell
./mvnw -pl notification-service -am verify -Pintegration
```

→ `JdbcProcessedEventStoreIT`, **7 tests** contre un vrai Postgres 18 (Testcontainers 2.0.5).
Premier `insert` : 1 ligne affectée. Second : 0.

```powershell
./infra/experiments/p4-t6-poison.ps1
```

```powershell
./infra/experiments/p4-t6-poison.ps1 -Temoin
```

Inspecter la DLT et rejouer :

```powershell
Invoke-RestMethod "http://localhost:8095/admin/dlt?limit=10" | Format-Table offset, eventId, retryStage, exceptionType
```

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8095/admin/dlt/replay" -ContentType "application/json" -Body '{"topic":"shop.order.events.v1.dlt","partition":0,"offsets":[23]}'
```

---

## Ce que cette phase ne couvre pas

- **L'ordre entre le topic principal et les étages de retry est perdu**, par construction.
  Un message réessayé passe après ceux publiés pendant son attente. C'est le prix du retry non
  bloquant, et il n'y a pas de moyen de l'éviter en gardant la partition libre. À l'intérieur
  d'un étage, l'ordre par agrégat est conservé : la clé est préservée et la partition est
  laissée au partitionneur.
- **L'idempotence n'est pas atomique avec l'effet métier.** Un crash entre le marquage et
  l'échec laisse une marque orpheline. C'est la phase 5.
- **Un refus métier n'est pas une panne.** Un paiement refusé ne va pas en DLT : c'est la
  branche de compensation de la saga, publiée comme un événement. Phase 6.
- **La console `/admin/dlt` n'est pas authentifiée.** Les services sont liés au loopback
  (`server.address`), ce qui rend cette absence acceptable dans un labo — et rien d'autre.
  P12-T8.
- **Une seule généralisation reste à faire** : trois listeners pour un handler tiennent dans ce
  service. En phase 6 il y aura 5 services × 2 étages. À revoir en P6-T1, pas avant — on ne
  connaîtra la bonne forme de l'abstraction qu'à ce moment.

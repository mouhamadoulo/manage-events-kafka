# Design — Phase 4 : fiabilité consumer (retry, DLQ, idempotence)

Date : 2026-11-23
Tâches couvertes : **P4-T1 à P4-T8** de [docs/TASKS.md](../../TASKS.md)
Statut : **approuvé par l'utilisateur le 2026-11-23** — clé primaire composite validée
Révision : v2, après relecture (§11 liste ce que la relecture a changé)

---

## 1. Intention et pourquoi

À la fin de la phase 3, le producteur est fiable : on sait ce qu'un `send()` garantit, et un
échec est visible. Le consumer, lui, est resté volontairement nu depuis P1-T3 :

- `KafkaJsonSchemaDeserializer` sans garde-fou → **un seul message mal formé bloque la
  partition pour toujours** : la désérialisation échoue avant le listener, l'offset n'est
  jamais commité, le même message est repollé indéfiniment.
- Aucune idempotence → Kafka livre **au moins une fois**. Un rebalancing, un redémarrage ou
  un reset d'offsets (déjà mesuré en P1-T6 : 272 événements retraités) rejoue les mêmes
  événements, et chaque rejeu produit un nouvel effet métier.

La phase 4 corrige les deux. C'est le prérequis de tout le reste : la saga de la phase 6 est
une chaîne de consumers, et un consumer qui bloque sa partition bloque la saga entière.

## 2. Objectifs

1. Un message empoisonné ne bloque **jamais** une partition. Il finit dans `.dlt` avec de
   quoi diagnostiquer, et le flux continue.
2. Une erreur **transitoire** est réessayée avec un délai croissant, sans monopoliser le
   topic principal.
3. Le même `eventId` livré deux fois produit **un seul** effet métier.
4. Un message en DLT peut être rejoué, et le rejouer deux fois ne produit qu'un effet.
5. Chaque mécanisme reste **lisible** : on doit pouvoir pointer la ligne de code qui fait le
   retry, celle qui route vers la DLT, celle qui déduplique.

## 3. Non-objectifs

- **Pas de retry sur les erreurs métier définitives.** Un paiement refusé n'est pas une
  panne : c'est la branche de compensation de la saga (phase 6). La DLT est pour les
  **défauts techniques**, pas pour les refus métier.
- **Pas d'exactly-once.** L'idempotence consumer de cette phase est applicative (table
  `processed_event`). Les transactions Kafka et l'EOS sont la phase 7.
- **Pas de couplage base + Kafka dans une transaction.** Le problème du dual-write est la
  phase 5 (outbox). Ici, l'écriture de `processed_event` et l'effet métier ne sont pas
  atomiques, et c'est documenté comme une limite assumée (§7.2).
- **Pas de Testcontainers généralisé.** Le profil `integration` complet est P10-T3. Cette
  phase n'ajoute qu'un `*IT` pour la table `processed_event`.
- **Pas d'authentification sur l'endpoint de rejeu.** Comme tout le labo (P12-T8).
- **Pas de généralisation prématurée aux 5 services de la saga.** La phase 4 rend le
  mécanisme réutilisable (beans dans `common-kafka`, import explicite), mais ne l'applique
  qu'à `notification-service`. La phase 6 verra si l'abstraction tient.

## 4. Anti-objectifs

Ce qui rendrait la phase ratée même si les tests passent :

- Un mécanisme qui marche mais dont on ne sait pas dire **pourquoi** il marche. Interdit :
  copier une annotation qui fait tout.
- Un `processed_event` écrit **avant** l'effet métier sans compensation sur échec : le retry
  serait alors confondu avec un doublon et le travail serait **perdu en silence**. C'est le
  piège central de cette phase, traité en §7.2, et la relecture a montré qu'il se reforme
  tout seul à l'interaction avec le différé (§7.4).
- Un retry qui bloque le thread du consumer du topic principal.
- Une DLT sans les informations de diagnostic : un message qu'on ne sait pas rejouer n'est
  pas une DLT, c'est une poubelle.
- Des dépendances ajoutées à `common-kafka` qui empêchent les 5 autres services de démarrer.

## 5. Contraintes trouvées dans le projet

| Contrainte | Conséquence |
|---|---|
| `create-topics.ps1` crée déjà `.retry-1`, `.retry-2`, `.dlt` pour les 4 topics métier | Les noms sont figés. Un mécanisme qui impose ses propres noms doit être adapté. |
| `register-schemas.ps1` a enregistré 4 sujets, dont `shop.order.events.v1.dlt-OrderPlaced` | Publier dans `.dlt` avec le sérialiseur JSON Schema fonctionne, `auto.register.schemas=false` inclus. |
| `auto.create.topics.enable=false` sur le broker | Aucun topic n'est créé par effet de bord : toute faute de nom échoue. |
| `TopicRecordNameStrategy` : le nom du topic fait partie du sujet | Un topic de retry a besoin de son propre sujet (déjà fait). |
| **Les 6 services dépendent de `common-kafka`**, et 5 n'ont pas de base | Toute dépendance JDBC/Flyway en portée compile dans `common-kafka` casse leur contexte (§7.8). |
| `common-kafka` n'a aujourd'hui **aucun bean Spring**, pas de `src/main/resources` | La phase 4 y met les premiers. Le mode de branchement est une décision structurante (§7.9). |
| Base `notifications` déjà créée par `init-databases.sh` | Rien à ajouter au compose. |
| `notification-service` porte deux modèles de consommation (P1-T7) | Le travail cible la branche `@KafkaListener`. Le profil `cloud-stream` garde le comportement nu, comme témoin — l'import étant explicite, il le reste vraiment. |
| Headers `eventId` / `eventType` / `correlationId` publiés par `KafkaEventPublisher` | L'idempotence peut lire l'`eventId` **sans désérialiser** le payload. |
| `order-service` a déjà un profil `lab` (`LabProducerController`) | L'injection de panne de T4 s'y greffe sans polluer le chemin normal (§7.5). |

### Conflit vérifié : `@RetryableTopic` et nos noms de topics

`javap -c` sur `spring-kafka 4.2.0`, classe `DestinationTopicPropertiesFactory` :
`joinWithRetrySuffix(long)` concatène le suffixe de retry avec un `-` **en dur**, puis
l'index (`SUFFIX_WITH_INDEX_VALUE`) ou le délai en ms (`SUFFIX_WITH_DELAY_VALUE`), et
`RetryTopicConstants.DEFAULT_RETRY_SUFFIX = "-retry"`.

Avec `retryTopicSuffix = ".retry"`, l'annotation produit donc `shop.order.events.v1.retry-0`
et `.retry-1` — décalés d'un cran par rapport à nos topics et à nos sujets déjà enregistrés.
Il faudrait soit un `RetryTopicNamesProviderFactory` sur mesure, soit renommer les 12 topics
et réenregistrer les sujets.

## 6. Approches considérées

### A — Tout à la main *(retenue)*

`DefaultErrorHandler` + backoff exponentiel pour les retries bloquants rapides, puis
`DeadLetterPublishingRecoverer` avec un résolveur de destination maison
(`topic → .retry-1 → .retry-2 → .dlt`), et un listener par topic de retry dont le délai est
appliqué par la mise en pause de la partition.

- **Pour** : chaque étage est une ligne de code qu'on peut montrer. Les noms de topics
  existants sont respectés, aucun renommage, aucun sujet à réenregistrer. Conforme à la
  règle 7 de CLAUDE.md (pas de raccourci qui masque le pattern).
- **Contre** : plus de code que l'annotation, et il faut assembler soi-même le différé —
  c'est précisément la partie instructive.

### B — `@RetryableTopic`

L'annotation du framework gère topics de retry, backoff non bloquant, DLT et headers.

- **Pour** : une dizaine de lignes de configuration.
- **Contre** : le mécanisme est caché, et le conflit de nommage ci-dessus impose soit du code
  sur mesure, soit un renommage de l'infra déjà vérifiée en P0-T6 et P2-T3.

### C — Les deux côte à côte, sous profils

- **Pour** : le plus instructif. **Contre** : une neuvième tâche hors périmètre.

**Décision (de l'utilisateur, 2026-11-23) : approche A.** La comparaison avec
`@RetryableTopic` est écrite dans la fiche 04 plutôt que codée.

## 7. Conception

### 7.1 Vue d'ensemble du chemin d'un message

```
shop.order.events.v1
  │
  ├─ ErrorHandlingDeserializer : une désérialisation qui échoue ne lève plus ;
  │  elle produit un record à valeur nulle + un header portant l'exception.
  │  → la partition n'est plus jamais bloquée.
  │
  ├─ IdempotentRecordInterceptor (common-kafka)
  │     lit le header eventId, INSERT ... ON CONFLICT DO NOTHING
  │     0 ligne insérée  → retourne null → le handler n'est PAS appelé, offset commité
  │     1 ligne insérée  → le record passe au handler
  │     échec du handler → failure() → forget() → la marque est retirée
  │
  ├─ OrderNotificationHandler : LE code métier, appelé par les 3 listeners
  │     succès → offset commité
  │     échec  → DefaultErrorHandler du conteneur principal
  │                 ├─ exception non-retryable → 0 retry, direct DLT
  │                 └─ exception retryable → 2 retries bloquants (200 ms, 400 ms + jitter)
  │                      puis DeadLetterPublishingRecoverer
  │
  └─ résolveur de destination
        depuis le topic principal → .retry-1
        depuis .retry-1           → .retry-2
        depuis .retry-2           → .dlt
        toujours, pour une erreur non-retryable → .dlt

.retry-1 (dû à T+5 s) / .retry-2 (dû à T+30 s)
    listeners dédiés, qui appellent le MÊME handler.
    Pas encore dû → NotYetDueException → la partition est mise en pause
    pour la durée restante, sans commit (§7.4).

.dlt : aucun listener. Lue à la demande par l'endpoint de rejeu (§7.6).
```

### 7.2 P4-T1 — `processed_event` et l'intercepteur

**Migration Flyway** à `classpath:db/migration/shared/V1__processed_event.sql`, portée par
`common-kafka` (ressource seule, sans dépendance Flyway transitive — cf. §7.8) :

```sql
create table processed_event (
    event_id       uuid        not null,
    consumer_group text        not null,
    source_topic   text        not null,
    processed_at   timestamptz not null default now(),
    primary key (event_id, consumer_group)
);
```

`notification-service` déclare
`spring.flyway.locations: classpath:db/migration/shared,classpath:db/migration`, de sorte que
les services de la saga (phase 6) héritent de la même table sans la recopier.

> **Écart avec CLAUDE.md §4, à valider par l'utilisateur.** §4 écrit
> `processed_event (event_id PK, consumer_group, processed_at)`. Une clé primaire sur
> `event_id` seul empêcherait deux groupes de consumers de traiter le même événement — le
> second verrait un doublon qui n'en est pas, et en phase 6 les 5 services de la saga lisent
> les mêmes topics dans 5 groupes différents. La clé proposée est donc **composite**
> `(event_id, consumer_group)`. `source_topic` reste **hors** de la clé : un record venu de
> `.retry-1` doit porter la même identité que sur le topic principal, sinon chaque étage de
> retry rejouerait l'effet.

**Composants, dans `common-kafka`** :

- `ProcessedEventStore` (interface, sans JDBC) :
  `boolean markProcessed(UUID eventId, String consumerGroup, String sourceTopic)` → `true` si
  la ligne a été insérée (donc à traiter), `false` si elle existait déjà (donc doublon) ;
  `void forget(UUID eventId, String consumerGroup)`.
- `JdbcProcessedEventStore` : implémentation `JdbcClient`, `INSERT ... ON CONFLICT DO NOTHING`,
  et on lit le **nombre de lignes affectées**. Pas de `SELECT` préalable : `SELECT` puis
  `INSERT` est une condition de course, deux instances du même groupe pouvant passer le test
  en même temps. La contrainte d'unicité est le seul arbitre.
- `IdempotentRecordInterceptor implements RecordInterceptor<String, Object>` :
  - `intercept()` lit le header `eventId`, appelle `markProcessed`, retourne `null` sur
    doublon (spring-kafka n'appelle alors pas le listener et commite l'offset).
  - Un record **sans** header `eventId` passe sans déduplication, avec un `WARN`. Choix
    assumé : l'absence du header est une faute du producteur, pas un défaut du message ; elle
    doit être visible, pas fatale.
  - Un record issu d'un **échec de désérialisation** (valeur nulle) passe sans déduplication :
    son `eventId` n'est pas fiable.
  - `failure(record, exception, consumer)` appelle `forget()`.

**Le piège central, et sa réponse ici.** Marquer avant de traiter rend le retry impossible :
le second passage verrait un doublon et sauterait le travail — perdu en silence. Trois
réponses possibles :

| Réponse | Verdict ici |
|---|---|
| Marquer **après** l'effet métier | Un crash entre les deux rejoue l'effet. Pire pour un effet non idempotent (un mail). |
| Marquer **dans la même transaction** que l'effet | La bonne réponse quand l'effet est dans la même base. C'est ce que fera la phase 5 côté écriture. Inapplicable ici : l'effet de `notification-service` est un compteur en mémoire. |
| Marquer avant, **effacer la marque sur échec** (`RecordInterceptor.failure`) | Retenue. Simple, et correcte tant qu'un crash brutal entre le marquage et l'échec reste acceptable — ce cas laisse une marque orpheline, donc un message sauté. |

**Conséquence sur tout le reste du design, trouvée en relecture.** `forget()` ne se déclenche
que si `failure()` est appelé, donc **uniquement si le handler lève une exception**. Tout
chemin qui sort du listener sans exception après être passé par l'intercepteur perd
définitivement le record. C'est pour cette raison que le différé de §7.4 lève
`NotYetDueException` au lieu de sortir silencieusement : la version initiale de ce design
faisait `seek` + `pause` + retour normal, et perdait tous les messages en attente.

Cette limite et son mécanisme sont **écrits dans la fiche 04** et dans le javadoc de
l'intercepteur : c'est la raison d'être de la phase 5.

### 7.3 P4-T3 — `DefaultErrorHandler` et classement des erreurs

- `ErrorHandlingDeserializer` enveloppe le `KafkaJsonSchemaDeserializer` côté valeur
  (`spring.kafka.consumer.value-deserializer` devient `ErrorHandlingDeserializer`, le délégué
  passe en `spring.deserializer.value.delegate.class`).
- Backoff : `initialInterval=200ms`, `multiplier=2.0`, 2 tentatives → 200 ms puis 400 ms.
  Volontairement court : le retry bloquant est là pour l'incident d'une demi-seconde (une
  reconnexion), pas pour attendre un service en panne. Au-delà, c'est le rôle des topics de
  retry. Vérifié par `javap` : `org.springframework.util.backoff.ExponentialBackOff` de
  spring-core 7.1.0 a gagné `setMaxAttempts(long)` **et** `setJitter(long)` ;
  `org.springframework.kafka.support.ExponentialBackOffWithMaxRetries` de spring-kafka 4.2.0
  existe encore. Le premier est retenu, pour le jitter : sans lui, N instances réessaient à la
  même milliseconde.
- **Non-retryables** (0 retry, DLT directe) : `DeserializationException`,
  `MethodArgumentResolutionException`, `MessageConversionException`, `ClassCastException`, et
  notre `NonRetryableEventException`. Un message qui ne se décode pas ne se décodera pas mieux
  dans 400 ms.
- **Retryables** : tout le reste, et explicitement `TransientProcessingException` (§7.5).
- `setCommitRecovered(true)` : une fois le message publié en DLT, son offset est commité ; sans
  cela il serait repollé.

### 7.4 P4-T4 — étages de retry et application du délai

Résolveur de destination, dans `common-kafka` :

```
RetryTopology.next(topic) :
    topic sans suffixe   → topic + ".retry-1"
    topic + ".retry-1"   → topic + ".retry-2"
    topic + ".retry-2"   → topic + ".dlt"
```

La partition cible est laissée au partitionneur (`partition = -1`) : la clé est conservée,
donc l'ordre par agrégat est conservé **à l'intérieur** d'un étage. L'ordre entre le topic
principal et les étages de retry, lui, est perdu par construction — c'est le prix du retry non
bloquant, et il est documenté.

**Délai, sans bloquer de thread et sans perdre le record.** Le listener de l'étage calcule
l'échéance (`x-origin-timestamp` du record + délai de l'étage). Si elle est passée, il appelle
le handler. Sinon, il **lève** `NotYetDueException(dureeRestante)`, et le conteneur de retry
est câblé pour traiter cette exception à part :

```
DefaultErrorHandler(recoverer, backOffParDefaut, new ContainerPausingBackOffHandler(pauseService))
    .setBackOffFunction((record, ex) ->
        ex instanceof NotYetDueException n ? new FixedBackOff(n.remainingMillis(), 3)
                                           : backOffParDefaut)
```

Classes vérifiées dans le jar `spring-kafka-4.2.0` :

| Classe | Rôle |
|---|---|
| `FailedRecordProcessor.setBackOffFunction(BiFunction<record, exception, BackOff>)` | le délai d'attente est calculé **par record**, donc exactement la durée restante — pas un pas fixe |
| `ContainerPausingBackOffHandler(ListenerContainerPauseService)` | met en pause au lieu de dormir |
| `ListenerContainerPauseService.pausePartition(container, topicPartition, Duration)` | la pause est **par partition** : les autres partitions de l'étage continuent |

Ce que ça donne : la partition est mise en pause pour la durée restante, **l'offset n'est pas
commité**, le record est redélivré à la reprise, il est alors dû, et il part au handler. Aucun
thread ne dort, et comme le chemin passe par une exception, `RecordInterceptor.failure()` est
appelé et la marque d'idempotence est retirée — les deux mécanismes ne se marchent plus dessus.

Deux pièges écartés par ce choix, tous deux à écrire dans la fiche 04 :

- **Dormir dans le listener** : un délai supérieur à `max.poll.interval.ms` (défaut 5 min) fait
  sortir l'instance du groupe et déclenche un rebalancing en boucle.
- **`pause` + `seek` + retour normal** (la première version de ce design) : sans exception,
  pas de `failure()`, donc pas de `forget()` ; au réveil l'intercepteur voit un doublon et
  saute le message. Et la reprise par `ListenerContainerIdleEvent` n'arrive jamais sous
  trafic continu, puisque l'événement n'est publié que si le conteneur ne reçoit **rien**.
  (`ListenerContainerPartitionIdleEvent` existe et répond au second point, pas au premier.)

Note pour la fiche : `@RetryableTopic` câble exactement les mêmes briques
(`ContainerPartitionPausingBackOffManager`, `KafkaBackoffAwareMessageListenerAdapter`). On les
assemble à la main pour les comprendre, et la fiche les nomme pour que le lecteur sache quoi
réutiliser en production.

### 7.5 Où vit le code métier, et comment on provoque un échec

**Un seul handler, trois listeners.** `notification-service` expose un bean
`OrderNotificationHandler` qui porte l'effet métier. Trois `@KafkaListener` l'appellent : le
topic principal, `.retry-1`, `.retry-2`. Les deux listeners de retry ajoutent uniquement le
test d'échéance de §7.4.

Les listeners restent **dans le service**, pas dans `common-kafka` : trois méthodes de quatre
lignes, contre une abstraction de handler générique dont on ne connaîtra la bonne forme qu'en
phase 6, quand 5 services × 2 étages existeront. YAGNI assumé, et noté dans la fiche comme
point à revoir en P6-T1.

**Injection de panne.** Rien ne peut échouer dans `notification-service` : l'effet métier est
un compteur en mémoire. Il n'y aurait donc aucun déclencheur pour T4, pour la branche « erreur
métier » de T5, ni pour T7. D'où un header de contrôle :

- `x-fail-times: N` sur le record → le handler lève `TransientProcessingException` les N
  premières fois qu'il voit cet `eventId`, puis réussit. Compteur en mémoire.
- `x-fail-fatal: true` → `NonRetryableEventException` immédiate, pour la DLT directe.
- **Un header, pas un champ du payload** : le schéma JSON est fermé
  (`additionalProperties: false`, cf. P2-T5), un champ en plus serait rejeté par le
  sérialiseur. Et un header se lit sans désérialiser, ce qui est déjà le parti pris du projet.
- Côté producteur : paramètres optionnels sur le `LabProducerController` d'`order-service`,
  déjà sous profil `lab`. Le chemin normal (`POST /orders`) n'est pas touché.
- Côté consumer, la lecture de ces headers est active **sous profil `lab` uniquement**. En
  dehors, les headers sont ignorés — une panne ne doit pas pouvoir être commandée depuis un
  message.

### 7.6 P4-T5 — headers de la DLT

`DeadLetterPublishingRecoverer` construit avec **deux** `KafkaTemplate`, sélectionnés par type
de payload (constructeur `Map<Class<?>, KafkaOperations<?,?>>`, vérifié) :

| Type de payload | Template | Pourquoi |
|---|---|---|
| `byte[]` | sérialiseur `ByteArray` | Un échec de désérialisation n'a pas d'objet à republier, seulement les octets d'origine. Les passer au sérialiseur JSON Schema échouerait une seconde fois. |
| le reste | sérialiseur JSON Schema (sujet `.dlt-OrderPlaced` déjà enregistré) | Le message reste lisible par Kafka UI. |

Headers standard ajoutés par le recoverer : topic / partition / offset / timestamp d'origine,
FQCN et message de l'exception, trace. Headers ajoutés par nous via `setHeadersFunction` :

- `x-origin-topic` : le topic métier initial, posé **une seule fois** (si absent). Les headers
  `kafka_dlt-original-*` du framework sont réécrits à chaque étage ; le rejeu a besoin du
  point de départ, pas de l'étage précédent.
- `x-origin-timestamp` : l'instant de publication d'origine, posé une seule fois. C'est lui
  que §7.4 utilise pour les échéances — un délai compté depuis l'arrivée dans l'étage ne
  survivrait pas à un redémarrage.
- `x-retry-stage` : `0` pour le topic principal, `1`, `2` pour les étages.
- `x-failed-at` : instant de la mise en DLT.

### 7.7 P4-T7 — endpoint de rejeu

`POST /admin/dlt/replay` sur `notification-service`, corps
`{ "topic": "...dlt", "partition": 0, "offsets": [12, 13] }` :

1. un `KafkaConsumer` **sans groupe** (`assign` + `seek`, pas `subscribe`) — ne touche donc
   aucun offset commité ;
2. `ByteArrayDeserializer` : les octets sont relus et republiés **tels quels**. Les ids de
   schéma sont globaux (vérifié en P2-T3 : les 4 sujets portent `id=4`), donc un message
   valide sur `.dlt` reste valide sur le topic principal, et on ne risque pas une seconde
   désérialisation ratée. C'est aussi ce qui permet de rejouer un message empoisonné — il
   repartira en DLT, et c'est le comportement correct ;
3. borne dure à 100 messages par appel ;
4. republication sur le topic lu dans `x-origin-topic`, clé d'origine conservée, headers
   `eventId` / `eventType` / `correlationId` recopiés, headers de diagnostic retirés.

`GET /admin/dlt?topic=...&limit=20` pour inspecter avant de rejouer.

**Ce que le rejeu garantit, exactement.** La relecture a corrigé une formulation fausse : un
message arrivé en DLT après un échec métier a vu sa marque d'idempotence **effacée** par
`forget()`. Le rejeu le **retraite** donc, et c'est le but — sinon le rejeu ne servirait à
rien. La propriété vraie est : **rejouer deux fois ne produit qu'un effet.** Le premier rejeu
traite et repose la marque, le second est sauté par `processed_event`. C'est ce que vérifie
T7, en deux appels.

Cet endpoint est une console d'administration sans authentification, comme le reste du labo
(Kafka UI avec `DYNAMIC_CONFIG_ENABLED`, Postgres en `eda/eda`). Il n'écoute que sur
`127.0.0.1`, la borne de 100 messages le rend inoffensif pour la mémoire, et P12-T8 le passera
derrière un jeton. Écrit tel quel dans le javadoc.

### 7.8 P4-T6 — expérience message empoisonné

`infra/experiments/p4-t6-poison.ps1`, sur le modèle des scripts existants :

1. 10 messages valides par `POST /orders`, tous sur la même clé donc la même partition ;
2. un message **brut** sans octet magique injecté sur cette partition par
   `kafka-console-producer` (via fichier + `docker cp`, à cause du BOM de PowerShell 5.1 déjà
   documenté) ;
3. 10 messages valides de plus, même clé ;
4. mesures : lag du groupe par partition avant / après, contenu de `.dlt`, compteur `/stats`.

Résultat attendu : **1 message en `.dlt`, 20 traités, lag retombé à 0.** Le témoin « avant »
est obtenu en désactivant `ErrorHandlingDeserializer` par variable d'environnement — le
profil `cloud-stream` ne convient pas, puisque l'import explicite de §7.9 le laisse déjà nu
pour d'autres raisons et qu'il n'utilise pas le même conteneur.

### 7.9 Dépendances et branchement

**Dépendances.** `common-kafka` est au classpath des 6 services, dont 5 sans base. Y mettre
`spring-boot-starter-jdbc` et `flyway-core` en portée compile met Hikari,
`DataSourceAutoConfiguration` et `FlywayAutoConfiguration` chez tout le monde, et les 5
contextes échouent sur `Failed to determine a suitable driver class`. Donc :

| Module | Dépendance | Portée | Pourquoi |
|---|---|---|---|
| `common-kafka` | `spring-boot-starter-jdbc` | **`<optional>true</optional>`** | `JdbcClient` compile ici, mais la dépendance n'est pas transitive : les 5 services sans base ne voient ni Hikari ni l'auto-configuration. |
| `common-kafka` | `flyway-core` | **`<optional>true</optional>`** | idem. La migration reste une ressource du jar, lisible par le Flyway du service qui en a un. |
| `notification-service` | `spring-boot-starter-jdbc`, `flyway-core`, `flyway-database-postgresql`, `org.postgresql:postgresql` | compile / runtime | déclarées explicitement, service par service. |
| `notification-service` | Testcontainers `postgresql` | test | hôte du seul `*IT` de la phase. `common-kafka` est un module bibliothèque, sans `spring-boot-maven-plugin` : le test d'intégration vit là où vit la `DataSource`. |
| `common-events` | — | — | les constantes de headers (`eventId`, `eventType`, `correlationId`) **déménagent** de `KafkaEventPublisher` vers un `EventHeaders` dans `common-events` : l'intercepteur en a besoin, et un nom de header est un élément de contrat. |

**Branchement : import explicite, pas d'auto-configuration.** `common-kafka` expose une
`@Configuration ConsumerReliabilityConfiguration` (error handler, intercepteur, les deux
`KafkaTemplate`, le `DeadLetterPublishingRecoverer`, le `ListenerContainerPauseService`), et
`NotificationServiceApplication` l'importe par `@Import`. Pas de
`AutoConfiguration.imports` :

- `com.formation.eda.kafka` est hors du scan de `com.formation.eda.notification` : aujourd'hui
  rien ne s'active par accident, et on veut que ça reste vrai ;
- une auto-configuration sans condition s'appliquerait aussi aux consumers d'`order-service`
  et au binder Kafka Streams d'`analytics-service`, ce qui n'est pas voulu ;
- le profil `cloud-stream` doit rester **nu** pour servir de témoin (§5) ;
- une ligne `@Import` est lisible par le lecteur de la fiche. Une auto-configuration est une
  magie à expliquer.

C'est la décision que les 5 services de la phase 6 hériteront : chacun importera
explicitement ce qu'il veut.

## 8. Vérification

Une tâche n'est faite qu'après exécution réelle de sa commande (règle 6 de CLAUDE.md). Sortie
collée dans `docs/TASKS.md`.

| Tâche | Comment on vérifie |
|---|---|
| T1 | `./mvnw -pl common-kafka -am test` ; `JdbcProcessedEventStoreIT` (dans `notification-service`, sous `-Pintegration`) montre 1 ligne puis 0 ligne affectée. |
| T2 | Test : deux livraisons du même `eventId` → handler appelé une fois, compteur à 1. Écrit **avant** l'intercepteur. |
| T3 | Test : exception non-retryable → 1 appel, 0 retry ; exception retryable → 3 appels. |
| T4 | Expérience avec `x-fail-times: 3` : horodatages de publication dans `.retry-1` / `.retry-2` → écarts ≈ 5 s et ≈ 30 s **mesurés**. Et un test qui prouve qu'un record pas encore dû n'est pas perdu : il est bien traité après la pause, une seule fois. |
| T5 | `kafka-console-consumer --property print.headers=true` sur `.dlt` : headers d'origine, exception, `x-origin-topic`, `x-origin-timestamp`, `x-retry-stage`. Deux cas : échec métier (payload JSON Schema) et échec de désérialisation (payload brut). |
| T6 | Script : 1 en DLT, 20 traités, lag à 0. Et le témoin sans `ErrorHandlingDeserializer` : lag qui ne redescend pas. |
| T7 | Message en DLT après épuisement des étages → rejeu n°1 : retraité, compteur +1. Rejeu n°2 du même offset : **aucun** effet nouveau. |
| T8 | Fiche écrite avec les mesures réelles, pas des ordres de grandeur. |
| Global | `./mvnw clean verify` → BUILD SUCCESS, 9 modules, les 6 tests de contexte verts (`JAVA_HOME` sur le JDK 25, cf. CLAUDE.md §5). |

## 9. Ordre de travail et commits

Un commit par tâche, Conventional Commits, scope = module (règle 4). Tests avant
implémentation sur toute la logique de fiabilité (règle 5).

1. `refactor(common-events)` : `EventHeaders` déménagé.
2. `feat(common-kafka)` : T1 + T2 — store, intercepteur, migration, tests d'abord.
3. `feat(common-kafka)` : T3 — `ErrorHandlingDeserializer`, `DefaultErrorHandler`, classement.
4. `feat(common-kafka)` : T4 + T5 — étages, résolveur, différé par pause, DLPR, headers.
5. `feat(notification-service)` : handler unique, 3 listeners, `@Import`, `application.yml`
   commenté propriété par propriété (règle 4 de la section pédagogie), injection de panne
   sous profil `lab`.
6. `feat(infra)` : T6 — expérience message empoisonné.
7. `feat(notification-service)` : T7 — endpoint de rejeu.
8. `docs` : T8 — fiche `04-retry-dlq-idempotence.md`, mise à jour de `docs/TASKS.md`, et
   correction de CLAUDE.md §4 sur la clé composite.

## 10. Laissé à l'implémentation

- Valeurs exactes des délais si 5 s / 30 s rendent l'expérience T4 trop lente à observer. Le
  rapport entre étages compte plus que les valeurs.
- `concurrency` des conteneurs de retry.
- Forme précise du corps de `POST /admin/dlt/replay` (offsets, ou plage).
- Nom exact du paramètre d'environnement qui désactive `ErrorHandlingDeserializer` pour le
  témoin de T6.

## 11. Ce que la relecture a changé

Un relecteur a lu la v1 de ce document sans accès à la conversation. Sept points, tous
intégrés ci-dessus :

1. **Le différé contredisait l'idempotence.** `pause` + `seek` + retour normal ne lève pas
   d'exception, donc pas de `forget()`, donc le record est classé doublon au réveil et perdu.
   → le différé lève maintenant `NotYetDueException` et passe par le `BackOffHandler` (§7.4).
2. **La réinvocation du code métier n'était pas décrite.** → un handler, trois listeners, dans
   le service (§7.5).
3. **La vérification de T7 était fausse** (« sauté par l'idempotence » : impossible après
   `forget()`). → la propriété vraie est « deux rejeux, un seul effet », vérifiée en deux
   appels (§7.7).
4. **Aucun déclencheur d'échec retryable n'existait** dans un service dont l'effet est un
   compteur. → headers `x-fail-times` / `x-fail-fatal`, sous profil `lab` (§7.5).
5. **`spring-boot-starter-jdbc` dans `common-kafka` cassait les 5 autres services.** → portée
   `optional`, pas de nouveau module, carte des modules de CLAUDE.md §2 inchangée (§7.9).
6. **Le mode de branchement des beans n'était pas décidé.** → `@Import` explicite, jamais
   d'auto-configuration (§7.9).
7. **La reprise par `ListenerContainerIdleEvent` ne marche pas sous trafic continu.** → sans
   objet, le point 1 ayant supprimé ce mécanisme.

Plus deux corrections mineures : le `*IT` vit dans `notification-service` et non dans
`common-kafka` (§7.9), et la clé primaire composite est soumise à validation (§7.2).

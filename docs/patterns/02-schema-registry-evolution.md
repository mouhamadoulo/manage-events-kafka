# 02 — JSON Schema, Schema Registry, évolution de contrat

> Phase 2. Mesures relevées le 2026-11-22 sur l'infra locale (Kafka 4.4.0,
> `confluentinc/cp-schema-registry:8.4.1`, sérialiseurs `io.confluent` 8.4.1).

---

## Le problème

En phase 1, `order-service` publiait du JSON libre. Rien n'empêchait :

- de renommer `totalAmount` en `amount` et de casser trois consumers en silence ;
- d'envoyer `"currency": "eur"` là où tout le monde attend `EUR` ;
- de changer le format d'une date sans que personne ne s'en aperçoive avant la production.

Un topic est une **API** : des clients qu'on ne connaît pas lisent ses messages, parfois des
mois plus tard. Une API sans contrat déclaré n'est pas une API, c'est une convention orale.

---

## Le mécanisme

```
   producteur                                      consumer
  ┌──────────┐   1. quel schéma ?   ┌───────────┐   3. quel schéma ?  ┌──────────┐
  │ événement│ ───────────────────▶ │  Schema   │ ◀────────────────── │ événement│
  └────┬─────┘      (sujet)         │ Registry  │      (id du schéma) └────▲─────┘
       │                            └───────────┘                         │
       │  2. publie : [0x00][id du schéma sur 4 octets][JSON]              │
       └──────────────────────────────────────────────────────────────────▶│
                              topic Kafka
```

Le message porte **5 octets de préambule**, vérifiés sur le topic :

```
$ docker exec kafka .../kafka-console-consumer.sh --topic shop.order.events.v1 ... | od -An -c
  \0  \0  \0  \0 004   {   "   e   n   v   e   l   o   p   e   "
```

`\0` = octet magique, `004` = schéma n° 4 en big-endian, puis le JSON. Le consumer n'a donc
rien à deviner : il lit l'identifiant, demande le schéma au registre, et le met en cache.

### Trois notions à ne pas confondre

| Notion | Ce que c'est | Portée |
|---|---|---|
| **Schéma** | le contrat lui-même, identifié par un `id` | **global** au registre |
| **Sujet** | un nom sous lequel des versions de schéma se succèdent | par topic (ici) |
| **Version** | le rang d'un schéma dans un sujet | par sujet |

Observé à l'enregistrement : les quatre sujets du topic de base, des deux topics de retry et
de la DLT reçoivent **le même `id=4`** et chacun sa **version 1**. Un schéma identique n'est
stocké qu'une fois ; les sujets sont des pointeurs vers lui.

### Le nom du sujet n'est pas un détail

Avec `TopicRecordNameStrategy`, `sujet = <topic>-<titre du schéma>` :

```
shop.order.events.v1-OrderPlaced
shop.order.events.v1.retry-1-OrderPlaced
shop.order.events.v1.dlt-OrderPlaced
```

Un topic peut donc transporter **plusieurs types d'événements**, chacun avec son contrat —
indispensable dès la phase 6, où `shop.order.events.v1` portera aussi `OrderCancelled` et
`OrderCompleted`. `TopicNameStrategy`, le défaut (`<topic>-value`), n'autorise qu'un schéma
par topic.

Conséquence mesurée, découverte par un test : **le nom du topic fait partie du sujet**, donc
la DLT a besoin de son propre enregistrement. Sans lui, publier le même événement sur
`shop.order.events.v1.dlt` échoue sur `Error retrieving latest version`. Le script
d'enregistrement crée les quatre sujets.

---

## Le code clé

### Le contrat est un fichier, pas une classe

Les sérialiseurs Confluent savent dériver un schéma d'une classe Java. C'est tentant, et
c'est un piège — voici ce que la dérivation produit pour `OrderPlaced` :

```json
{"title":"Order Placed","properties":{"orderId":{"oneOf":[{"type":"null","title":"Not included"},{"type":"string"}]}, ...}}
```

Un titre **avec une espace** (donc un sujet `shop.order.events.v1-Order Placed`), chaque champ
noyé dans un `oneOf [null, …]`, et aucune contrainte métier. Le contrat publié dépendrait des
conventions de nommage d'une bibliothèque.

Le schéma écrit à la main dit ce que le type Java ne sait pas dire :

```json
"currency": { "type": "string", "pattern": "^[A-Z]{3}$" },
"lines":    { "type": "array", "minItems": 1, "maxItems": 100, ... },
"occurredAt": { "type": "string", "format": "date-time" }
```

Et le sérialiseur passe ce schéma explicitement, par le mécanisme d'enveloppe de Confluent :

```java
JsonSchema schema = catalog.schemaFor(eventType);
JsonNode payload = jackson2.valueToTree(event);
return delegate.serialize(topic, JsonSchemaUtils.envelope(schema, payload));
```

### Quatre réglages contre les défauts

```java
merged.put("auto.register.schemas", false);        // défaut true
merged.put("use.latest.version", true);
merged.put("latest.compatibility.strict", false);
merged.put("json.fail.invalid.schema", true);      // défaut false !
```

`auto.register.schemas=true` signifie : *le premier producteur qui démarre impose le contrat*.
Une faute de frappe dans un nom de champ devient la version 1 du sujet, sans revue. Ici
l'enregistrement est un acte explicite, versionné dans git :
`infra/schemas/register-schemas.ps1`.

`json.fail.invalid.schema=false` par défaut est le réglage le plus surprenant : **sans lui, un
message hors contrat part sans un mot.**

Le test qui le montre :

```java
OrderPlaced suspicious = orderPlaced("eur");   // le record accepte : chaîne non vide
assertThatThrownBy(() -> serializer.serialize(TOPIC, suspicious))
        .hasMessageContaining("Error serializing JSON message");
```

Deux filets à mailles différentes, et c'est voulu : le contrat réseau peut être plus strict
que le type Java, et il s'applique aussi aux producteurs qui ne sont pas en Java.

---

## Qui valide quoi — la méprise à éviter

**Le broker ne valide rien.** Apache Kafka ne lit jamais le contenu des messages : pour lui,
une valeur est un tableau d'octets. Le Schema Registry, lui, ne voit **que des schémas** : il
vérifie la compatibilité entre versions d'un sujet, jamais la conformité d'un message.

La validation constatée plus haut est faite **par le client producteur**, parce qu'on le lui a
demandé. Un producteur mal configuré — ou simplement un `kafka-console-producer` — écrit ce
qu'il veut dans le topic. Le contrat est une discipline outillée, pas une barrière.

---

## Ce que l'évolution autorise

`infra/schemas/p2-t5-evolution.ps1` soumet trois variantes au registre, sur un sujet de
laboratoire supprimé à la fin.

| Variante | Verdict | Code d'erreur du registre |
|---|---|---|
| **A** ajout d'un champ **optionnel** (`channel`) | **compatible** | — |
| **B** ajout d'un champ **requis** | refusé | `REQUIRED_PROPERTY_ADDED_TO_UNOPEN_CONTENT_MODEL` |
| **C** retrait d'un champ (`currency`) | refusé | `PROPERTY_REMOVED_FROM_CLOSED_CONTENT_MODEL` |

**BACKWARD se lit dans un seul sens** : un consumer équipé du **nouveau** schéma doit pouvoir
lire les **anciens** messages. D'où l'ordre de déploiement implicite — **consumers d'abord,
producteurs ensuite**.

B découle directement de cette définition : les anciens messages n'ont pas `channel`, donc un
schéma qui l'exige ne peut pas les lire.

C est moins intuitif, et c'est le **coût de `additionalProperties: false`**. Avec un modèle de
contenu fermé, retirer un champ rend les anciens messages illisibles : ils portent un champ
que le nouveau schéma n'autorise plus. Le modèle fermé attrape les fautes de frappe d'un
producteur, mais rigidifie l'évolution. C'est un arbitrage à faire consciemment, pas un
réflexe.

Et la réciproque n'est jamais garantie par BACKWARD : un consumer resté en v1 qui reçoit un
message v2 voit un champ inconnu. D'où, côté consumer :

```yaml
json.fail.unknown.properties: false   # défaut true
```

Sans ce réglage, publier la variante A — pourtant déclarée compatible — casserait tous les
consumers non encore redéployés.

---

## Trois incidents rencontrés, et ce qu'ils enseignent

### 1. `Unknown magic byte!` — un changement de format est une migration

Au premier démarrage après la bascule, le consumer est tombé sur les messages de la phase 1 :

```
Caused by: java.lang.IllegalArgumentException: Unknown magic byte!
java.lang.IllegalStateException: This error handler cannot process 'SerializationException's
directly; please consider configuring an 'ErrorHandlingDeserializer' …
```

Du JSON nu ne commence pas par `0x00`. Les anciens messages sont devenus **définitivement
illisibles** par le nouveau consumer, et la partition s'est bloquée : offset jamais commité,
même message repollé en boucle. Le *poison message* de la phase 4, en vrai, sans l'avoir
cherché.

Deux issues possibles, et le choix est structurant :

- **purger le topic** — acceptable ici, c'était un laboratoire ;
- **publier sur `shop.order.events.v2`** en parallèle — c'est précisément pourquoi la
  convention de nommage porte un numéro de version (CLAUDE.md §2). En production, avec des
  consumers qu'on ne contrôle pas, c'est la seule voie.

Ce que le Schema Registry ne protège pas : il garantit la compatibilité **entre versions d'un
schéma**, pas entre deux **formats de transport**.

### 2. `error code: 40403` — supprimer un schéma casse le passé

Après avoir supprimé définitivement les sujets (`?permanent=true`) pour les réenregistrer, le
consumer a échoué sur `Schema not found; error code: 40403`. Cause : le producteur **encore en
cours d'exécution** gardait en cache l'id 2 et continuait de l'écrire dans ses messages, alors
que cet id n'existait plus.

Un identifiant de schéma est inscrit **dans chaque message, pour toute la durée de rétention**.
Supprimer un schéma pour de bon rend illisibles tous les messages qui le référencent. La
suppression permanente n'est pas une opération de nettoyage, c'est une amputation du passé.

### 3. Le format des dates change selon la bibliothèque qui sérialise

Les serdes Confluent 8.4.1 fonctionnent en **Jackson 2** ; Spring Boot 4 est en **Jackson 3**.
Il n'y a pas de conflit de versions — les packages diffèrent (`tools.jackson` contre
`com.fasterxml`) — mais les deux n'écrivent pas pareil :

| Sérialisé par | `occurredAt` |
|---|---|
| Jackson 3 (phase 1) | `"2026-11-22T09:59:01.834821200Z"` |
| Jackson 2 sans module java.time (défaut Confluent) | `1795344536.104638100` |

Sans réglage, passer au Schema Registry **changeait silencieusement le type d'un champ du
contrat** : de `string` à `number`. Le sérialiseur convertit donc l'événement avec un mapper
Jackson 2 explicitement configuré (`JavaTimeModule`, `WRITE_DATES_AS_TIMESTAMPS` désactivé).

---

## Comment le casser, pour voir l'échec

| Expérience | Conséquence attendue |
|---|---|
| Mettre `json.fail.invalid.schema=false` et publier `"currency": "eur"` | Le message part. Les consumers reçoivent une devise hors contrat et chacun improvise. |
| Repasser à `auto.register.schemas=true` et renommer un champ | Le registre accepte une version 2 sans revue, ou refuse au démarrage du service — un déploiement devient un acte de modification de contrat. |
| Publier avec `TopicNameStrategy` | Un seul schéma par topic : le deuxième type d'événement du topic écrase le contrat du premier. |
| Enregistrer la variante B (champ requis) de force en `compatibility=NONE` | Les consumers à jour ne peuvent plus lire les anciens messages ; le rejeu depuis l'offset 0 (phase 1) devient impossible. |
| Arrêter le Schema Registry et publier | La publication échoue : le registre est devenu une dépendance du chemin critique. C'est le prix du contrat vérifié. |
| Mettre `json.fail.unknown.properties=true` et déployer un producteur v2 avant les consumers | Les consumers v1 refusent les messages v2, alors que le registre les déclare compatibles. |

---

## Comment vérifier

```powershell
./infra/schemas/register-schemas.ps1
```

```powershell
./infra/schemas/p2-t5-evolution.ps1
```

```powershell
Invoke-RestMethod http://localhost:8081/subjects
```

```powershell
Invoke-RestMethod http://localhost:8081/config/shop.order.events.v1-OrderPlaced
```

```powershell
Invoke-RestMethod http://localhost:8081/subjects/shop.order.events.v1-OrderPlaced/versions/1
```

Les 5 octets de préambule, sur le topic (Git Bash) :

```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic shop.order.events.v1 --from-beginning --max-messages 1 | od -An -c | head -2
```

Le contrat de sérialisation, sans Docker (Schema Registry en mémoire) :

```powershell
./mvnw -pl common-kafka -am test
```

---

## À retenir avant la phase 3

- Le contrat est un **fichier versionné dans git**, pas un effet de bord du code. Le dériver
  d'une classe revient à publier les conventions de nommage d'une bibliothèque.
- Le message porte l'**id du schéma**, pas le schéma. C'est 5 octets, et c'est ce qui rend le
  rejeu d'un vieux message possible des années après.
- La validation est faite par le **client**. Le broker ne lit rien, le registre ne voit que
  des schémas.
- `BACKWARD` = le nouveau schéma lit les anciens messages. Ordre de déploiement : **consumers
  d'abord**.
- `additionalProperties: false` durcit le contrat **et** l'évolution. Les deux à la fois.
- Changer de **format de transport** n'est pas une évolution de schéma : c'est une migration,
  et elle se traite par un topic `.v2` ou par une purge assumée.
- Le registre est désormais sur le chemin critique de toute publication.

Fiches voisines : [01 — partitions, clés, groupes](01-partitions-cles-groupes.md) ·
décisions : [ADR-001](../adr/ADR-001-choix-stack.md) (JSON Schema plutôt qu'Avro)

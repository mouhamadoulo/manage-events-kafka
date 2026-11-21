# ADR-001 — Choix de la stack et des patterns structurants

- **Date** : 2026-11-21
- **Statut** : accepté
- **Tâches liées** : P0-T3, P0-T5, P0-T8

## Contexte

Le projet est un laboratoire d'apprentissage de l'Event-Driven Architecture, pas un produit. Cela change les critères de décision : on privilégie ce qui **rend les mécanismes visibles**, pas ce qui va le plus vite à écrire. Un raccourci qui masque un pattern est un échec, même s'il marche.

Trois décisions devaient être prises avant d'écrire une ligne de code.

---

## Décision 1 — KRaft, pas ZooKeeper

**Retenu** : Kafka 4.4.0 en mode KRaft, nœud unique en rôle combiné (`broker,controller`).

**Pourquoi** : ZooKeeper est **supprimé** depuis Kafka 4.0. La question ne se pose plus, mais la conséquence est importante pour l'apprentissage : la majorité des tutoriels et des fichiers `docker-compose.yml` disponibles en ligne sont obsolètes. Tout exemple contenant un service `zookeeper` est antérieur à Kafka 4 et sera incompatible, y compris sur des points non liés à ZooKeeper.

**Conséquence** : un `CLUSTER_ID` fixe est obligatoire dans le compose (l'image Apache formate le stockage avec). Le passage à 3 nœuds et le quorum réel sont reportés en P12-T6, pour que l'expérience arrive quand les concepts de réplication sont acquis.

**Alternative écartée** : Redpanda. Plus léger, API compatible, mais il n'expose pas les mêmes outils d'introspection (`kafka-consumer-groups.sh`, topics internes, métadonnées du quorum). Or regarder le lag et les topics internes fait partie de l'exercice.

---

## Décision 2 — JSON Schema, pas Avro

**Retenu** : sérialisation JSON avec validation par **JSON Schema** et Confluent Schema Registry 8.4.1.

**Pourquoi** :

1. **Lisibilité.** Le payload reste lisible dans Kafka UI et dans la console. Pendant l'apprentissage, voir le message tel qu'il circule vaut plus que les octets économisés.
2. **Le concept est le même.** Enregistrement du schéma, sujets, niveaux de compatibilité, évolution, rejet d'un changement cassant : tout ce qui s'apprend ici se transpose à Avro ou Protobuf. Seul l'encodage change.
3. **Pas de génération de code.** Avro impose un cycle génération/build qui ajoute une couche d'outillage entre l'intention et le message. Avec JSON Schema, l'événement est un `record` Java écrit à la main, et le schéma est un fichier à côté.

**Ce qu'on perd, et qu'il faut savoir** : Avro est nettement plus compact et plus rapide, et son système de résolution de schéma (writer schema / reader schema) est plus riche. En production à fort volume, Avro ou Protobuf est le choix normal. Ce point sera documenté dans `docs/patterns/02-schema-registry-evolution.md` plutôt qu'escamoté.

**Conséquence technique** : les artefacts Confluent ne sont pas sur Maven Central → le dépôt `https://packages.confluent.io/maven/` est déclaré dans le POM parent. Et les sérialiseurs Confluent 8.4.1 peuvent dépendre de Jackson 2 alors que Spring Boot 4.2 embarque Jackson 3 ; le conflit est traité explicitement en P2-T6.

---

## Décision 3 — Saga chorégraphiée, pas orchestrée

**Retenu** : saga **chorégraphiée**. Chaque service réagit aux événements des autres. Aucun coordinateur.

**Pourquoi** : c'est la forme qui expose le plus de difficultés réelles, donc la plus instructive. Avec un orchestrateur, la logique de la transaction distribuée est centralisée dans un composant qui *connaît* le déroulé, et les problèmes durs (ordre partiel, idempotence de chaque participant, détection de blocage sans vue globale) sont absorbés par le framework. En chorégraphie, il faut les affronter un par un :

- aucun service ne connaît l'état global de la saga ;
- un timeout ne peut pas être détecté par un coordinateur, il faut le construire (P6-T7) ;
- la compensation est un chemin métier à part entière, pas un rollback technique ;
- la corrélation de bout en bout doit être propagée à la main dans les headers (P6-T8).

**Ce qu'on perd** : la lisibilité. Au-delà de 5 ou 6 participants, une saga chorégraphiée devient difficile à suivre, et c'est précisément le moment où un orchestrateur se justifie. Le fil rouge s'arrête à 4 participants, soit juste en dessous de ce seuil — le projet montre la chorégraphie à son point d'efficacité, et la comparaison des deux approches est écrite en P6-T9.

**Alternatives écartées** : Temporal, Camunda, Spring Statemachine. Excellents outils, mais ils résoudraient à notre place ce que nous voulons apprendre.

---

## Décision 4 — Multi-module Maven, un module par service

**Retenu** : un seul dépôt Maven multi-module, 6 services + 2 bibliothèques (`common-events`, `common-kafka`).

**Pourquoi** : un dépôt par service serait plus réaliste, mais multiplierait par six le coût de chaque changement de contrat pendant l'apprentissage. Le monorepo garde le cycle court.

**Garde-fou contre le faux découplage** : partager un module `common-events` crée un risque réel — un couplage de code qui masquerait le couplage de contrat. Deux règles compensent :

1. **une base de données par service** (`orders`, `payments`, `inventory`, `shipping`, `notifications`), donc aucune jointure possible entre services ;
2. **aucun appel REST entre services**, la seule voie de communication est Kafka.

`common-events` reste strictement limité aux contrats, sans logique métier.

---

## Décision 5 — Java 25 et Spring Boot 4.2.0

**Retenu** : Java 25 LTS, Spring Boot 4.2.0, Spring Cloud 2026.0.0.

**Pourquoi cet alignement précis** : le BOM `spring-cloud-dependencies:2026.0.0` déclare lui-même `spring-boot.version` = 4.2.0. Aucun arbitrage de version à faire, ce qui élimine une source classique d'erreurs sur ce type de projet.

**Le coût, assumé** : Spring Boot 4 et Jackson 3 sont récents. Les starters ont été redécoupés (`spring-boot-starter-kafka` existe maintenant, les starters web sont modulaires), Jackson a changé de packages (`tools.jackson.*`), Testcontainers est passé en 2.x, et Spring Cloud Stream 5 n'accepte plus que le modèle fonctionnel. Conséquence pratique : **aucun tutoriel trouvé en ligne ne sera directement applicable**. C'est inconfortable, mais cela force à lire la documentation de référence plutôt qu'à copier, ce qui sert l'objectif du projet. Les pièges connus sont recensés dans CLAUDE.md §5 pour éviter de les redécouvrir à chaque session.

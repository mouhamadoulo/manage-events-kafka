# 00 — Pourquoi l'event-driven, et ce qu'il coûte

> Fiche de la **Phase 0** (P0-T9). Prérequis conceptuel aux 13 phases suivantes.

## Le problème qu'on cherche à résoudre

Partons du fil rouge en version synchrone. `order-service` reçoit une commande et appelle les autres en REST :

```
POST /orders
   └─▶ POST payment-service/authorize      (300 ms)
          └─▶ POST inventory-service/reserve  (150 ms)
                 └─▶ POST shipping-service/ship  (200 ms)
                        └─▶ POST notification-service/send  (800 ms, SMTP lent)
```

Quatre conséquences, qui ne sont pas des détails d'implémentation :

**1. La latence s'additionne.** Le client attend 1450 ms, dont 800 ms pour un e-mail dont il n'a rien à faire.

**2. La disponibilité se multiplie.** Quatre services à 99,9 % chacun donnent 99,6 % pour la chaîne. Si `notification-service` est en panne, la commande échoue — alors qu'elle était payée et le stock réservé.

**3. Le couplage est topologique.** `order-service` connaît l'adresse, le contrat et le mode de défaillance de quatre services. Ajouter un cinquième destinataire (un service de fidélité, par exemple) modifie `order-service`, qui n'a rien à voir avec la fidélité.

**4. Les échecs partiels deviennent ingérables.** Le paiement a réussi, le stock a échoué. Qui rembourse ? Avec une chaîne REST, cette logique se retrouve dans le `catch` de l'appelant, où elle est elle-même sujette au crash.

## Ce que l'inversion change

En event-driven, `order-service` n'appelle personne. Il **publie un fait** :

```
POST /orders
   └─▶ shop.order.events.v1 : OrderPlaced          (5 ms, réponse au client)

        payment-service        ─┐
        notification-service   ─┤── consomment quand ils peuvent, à leur rythme
        analytics-service      ─┘
```

Gains, dans l'ordre d'importance :

- **Découplage de destinataire.** Le producteur ne sait pas qui consomme, ni combien sont là. Ajouter un service de fidélité = ajouter un consumer group. Zéro modification côté `order-service`. C'est le gain principal, et le plus durable.
- **Découplage temporel.** Un consumer arrêté ne fait pas échouer le producteur. Il rattrape son retard au redémarrage, parce que Kafka a conservé les messages.
- **Latence perçue.** Le client est libéré en 5 ms. Le travail réel se fait derrière.
- **Rejouabilité.** Un consumer peut relire l'historique depuis l'offset 0. Un bug corrigé dans une projection se répare en rejouant, pas en migrant des données à la main. C'est ce qui rend l'event sourcing possible (phase 8).

## Ce que ça coûte vraiment

C'est la partie que la plupart des présentations escamotent. Chaque bénéfice ci-dessus a une contrepartie.

| On gagne | On paie |
|---|---|
| Découplage temporel | **Cohérence à terme.** `GET /orders/{id}` juste après le POST peut renvoyer un état pas encore à jour. Il faut le concevoir, pas le subir. |
| Découplage de destinataire | **Plus personne ne connaît le déroulé complet.** Debugger demande de la corrélation distribuée (P6-T8), pas une stacktrace. |
| Rejouabilité | **Livraison au moins une fois.** Un message peut arriver deux fois. Tout consumer doit être idempotent (P4-T1), sinon on facture deux fois. |
| Résilience | **Le message empoisonné.** Un message non traitable bloque sa partition indéfiniment si rien n'est prévu. D'où retry et DLQ (phase 4). |
| Scalabilité horizontale | **L'ordre n'est garanti que par partition.** Deux événements de commandes différentes peuvent s'inverser. Le choix de la clé de partition devient une décision d'architecture (P1-T4). |
| Transaction locale rapide | **Le dual-write problem.** Écrire en base puis publier dans Kafka n'est pas atomique. Crash entre les deux = événement perdu. D'où l'outbox (phase 5). |

Et un coût qui n'apparaît dans aucun tableau : **l'exploitation**. Un système event-driven demande de surveiller le consumer lag, les DLQ, le retard des projections, la santé du quorum. Ce qui était un code HTTP 500 visible devient un retard silencieux. La phase 11 existe pour ça.

## Quand ne pas le faire

Honnêtement, le plus souvent. L'event-driven se justifie quand :

- plusieurs consommateurs indépendants s'intéressent au même fait, ou qu'on sait que d'autres viendront ;
- le découplage temporel a une valeur réelle (pics de charge, consommateurs lents ou peu fiables) ;
- l'historique des faits a une valeur en soi (audit, rejeu, analytique) ;
- le volume ou la charge justifie le découplage.

À l'inverse, pour un CRUD avec un seul consommateur et un besoin de cohérence forte, un appel synchrone est plus simple, plus rapide à écrire et plus facile à exploiter. **Kafka n'est pas une amélioration par défaut de REST** : c'est un autre modèle de cohérence, avec une facture.

Un cas intermédiaire fréquent : un seul service a besoin d'une réponse, mais trois autres ont besoin d'être informés. Appel synchrone pour le premier, événement pour les trois autres. Le choix se fait par interaction, pas pour le système entier.

## Ce que Kafka apporte, précisément

Kafka n'est ni une file ni un bus de messages, et confondre les trois mène à des erreurs de conception. C'est un **journal distribué, ordonné par partition, persistant et relisible**.

Trois propriétés en découlent, et elles structurent tout le reste du projet :

1. **Lire ne détruit pas.** Contrairement à une file classique, consommer un message ne l'enlève pas. Le message vit selon la politique de rétention du topic. C'est ce qui permet plusieurs consumer groups indépendants sur le même topic, et le rejeu.
2. **L'offset appartient au consumer.** Chaque groupe mémorise où il en est. Avancer, reculer, repartir de zéro : c'est une décision du consumer, pas du broker. Les expériences de P1-T6 reposent là-dessus.
3. **La partition est l'unité d'ordre et de parallélisme.** Et les deux sont liés : plus de parallélisme signifie moins de garanties d'ordre global. Ce compromis ne disparaît jamais, on choisit juste où le placer avec la clé de partition.

## À retenir avant la phase 1

- Un événement est un **fait accompli**, nommé au passé. `OrderPlaced`, pas `PlaceOrder`. Si c'est à l'impératif, c'est une commande adressée à quelqu'un de précis — un autre pattern, d'autres règles.
- Le producteur ne doit **rien supposer** de ses consommateurs. Dès qu'il en sait quelque chose, le découplage est perdu.
- **Au moins une fois** est le défaut réaliste. L'idempotence n'est pas une amélioration, c'est une condition de correction.
- **L'ordre global n'existe pas.** Seul l'ordre par partition existe. La clé décide ce qui est ordonné avec quoi.

---

**Suite** : phase 1 — partitions, clés, consumer groups et offsets, par l'expérience. → `01-partitions-cles-groupes.md`

-- Registre d'idempotence consumer (P4-T1).
--
-- Migration PARTAGEE : elle vit dans le jar de common-kafka et chaque service qui a une base
-- l'applique en ajoutant classpath:db/migration/shared a spring.flyway.locations. Les 5
-- services de la saga (phase 6) heritent donc de la meme table sans la recopier.
create table processed_event (
    event_id       uuid        not null,
    consumer_group text        not null,
    source_topic   text        not null,
    processed_at   timestamptz not null default now(),

    -- Cle COMPOSITE, et non event_id seul.
    -- WHY: en phase 6, les 5 services de la saga lisent les memes topics dans 5 groupes de
    -- consumers differents. Avec une cle sur event_id seul, le premier service a traiter un
    -- evenement le marquerait pour tous les autres, qui verraient un doublon qui n'en est pas.
    -- Chaque groupe doit traiter l'evenement exactement une fois, independamment des autres.
    primary key (event_id, consumer_group)
);

-- source_topic est journalise mais HORS de la cle.
-- WHY: un record venu de shop.order.events.v1.retry-1 est le MEME evenement que sur le topic
-- principal. S'il entrait dans la cle, chaque etage de retry rejouerait l'effet metier — le
-- retry produirait les doublons que cette table existe pour empecher.
comment on column processed_event.source_topic is
    'topic d''ou venait le record lors du premier traitement, pour diagnostic uniquement';

-- Pas d'index supplementaire : le seul acces est un INSERT ... ON CONFLICT DO NOTHING sur la
-- cle primaire, et un DELETE par cette meme cle. La purge des lignes anciennes viendra avec
-- la metrique d'age de la phase 5 (P5-T5), quand on saura ce qu'on veut en mesurer.

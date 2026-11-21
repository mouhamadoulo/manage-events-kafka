#!/bin/bash
# Cree une base par service. Execute une seule fois, au premier demarrage du volume Postgres.
# WHY: une base par service = pas de jointure possible entre services, le couplage ne peut
# passer que par les evenements Kafka. C'est la contrainte qui rend l'exercice honnete.
set -euo pipefail

for db in orders payments inventory shipping notifications; do
  echo "Creation de la base ${db}"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-EOSQL
    CREATE DATABASE ${db};
    GRANT ALL PRIVILEGES ON DATABASE ${db} TO ${POSTGRES_USER};
EOSQL
done

#!/usr/bin/env bash
# Creation explicite des topics du laboratoire.
# auto.create.topics.enable=false cote broker : rien n'existe sans passer par ici.
#
# Usage (Git Bash) :  bash infra/topics/create-topics.sh
# Equivalent PowerShell : infra/topics/create-topics.ps1
set -euo pipefail

CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:19092}"
KT="/opt/kafka/bin/kafka-topics.sh"

# create_topic <nom> <partitions> <politique> [retention_ms]
create_topic() {
  local name="$1" partitions="$2" policy="$3" retention="${4:-}"
  local -a cfg=("--config" "cleanup.policy=${policy}")
  if [[ -n "${retention}" && "${policy}" == "delete" ]]; then
    cfg+=("--config" "retention.ms=${retention}")
  fi
  if [[ "${policy}" == "compact" ]]; then
    # Compaction agressive : utile pour voir l'effet rapidement en dev.
    cfg+=("--config" "min.cleanable.dirty.ratio=0.1" "--config" "segment.ms=60000")
  fi

  echo "  -> ${name} (${partitions}p, ${policy})"
  docker exec -i "${CONTAINER}" "${KT}" \
    --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${name}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    "${cfg[@]}" > /dev/null
}

SEVEN_DAYS=604800000
THIRTY_DAYS=2592000000

echo "Topics metier :"
# Cle = orderId pour tout ce qui concerne la commande : garantit l'ordre par commande.
create_topic "shop.order.events.v1"     3 delete "${SEVEN_DAYS}"
create_topic "shop.payment.events.v1"   3 delete "${SEVEN_DAYS}"
# Cle = sku : l'ordre qui compte ici est celui des mouvements d'un article.
create_topic "shop.inventory.events.v1" 3 delete "${SEVEN_DAYS}"
create_topic "shop.shipping.events.v1"  3 delete "${SEVEN_DAYS}"

echo "Event store (phase 8) :"
# Compacte : la derniere valeur par cle est conservee indefiniment.
create_topic "shop.order.store.v1"      3 compact

echo "Retry + DLT (phase 4) :"
for base in shop.order.events.v1 shop.payment.events.v1 shop.inventory.events.v1 shop.shipping.events.v1; do
  create_topic "${base}.retry-1" 3 delete "${SEVEN_DAYS}"
  create_topic "${base}.retry-2" 3 delete "${SEVEN_DAYS}"
  # DLT a 1 partition : volume faible, et l'ordre global y est plus lisible pour le diagnostic.
  create_topic "${base}.dlt"     1 delete "${THIRTY_DAYS}"
done

echo
echo "Topics existants :"
docker exec -i "${CONTAINER}" "${KT}" --bootstrap-server "${BOOTSTRAP}" --list | sort

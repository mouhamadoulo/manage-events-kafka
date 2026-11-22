<#
  Experience P1-T4 — repartition des cles sur les partitions.

  Deux questions distinctes, qu'on confond souvent :
    1. une cle donnee tombe-t-elle TOUJOURS sur la meme partition ? (ordre)
    2. des cles differentes se repartissent-elles equitablement ?   (parallelisme)

  Partie A : N commandes via POST /orders. Chaque reponse contient la partition
             reellement attribuee par le broker. On compte.
  Partie B : la meme cle produite 5 fois sur un topic de laboratoire, pour voir que
             la partition ne change pas ; puis des messages sans cle.

  Prerequis : infra up, order-service lance sur 8091.
  Usage     : ./infra/experiments/p1-t4-partitions.ps1 [-Count 100]
#>
param(
    [int]$Count = 100,
    [string]$OrderServiceUrl = "http://localhost:8091",
    [string]$Topic = "shop.order.events.v1",
    [string]$LabTopic = "lab.partitioning.v1"
)

$ErrorActionPreference = "Stop"

Write-Host "=== Partie A : $Count commandes via POST /orders ===" -ForegroundColor Cyan

$results = foreach ($i in 1..[math]::Max($Count, 1)) {
    $body = @{
        customerId = "customer-$($i % 10)"
        lines      = @(@{ sku = "SKU-$($i % 5)"; quantity = 1; unitPrice = 9.99 })
        currency   = "EUR"
    } | ConvertTo-Json -Compress

    Invoke-RestMethod -Uri "$OrderServiceUrl/orders" -Method Post -ContentType 'application/json' -Body $body
}

Write-Host "`nRepartition des $Count commandes :"
$results | Group-Object partition | Sort-Object Name | ForEach-Object {
    $pct = [math]::Round(100 * $_.Count / $Count, 1)
    Write-Host ("  partition {0} : {1,4} messages  ({2} %)" -f $_.Name, $_.Count, $pct)
}

# Chaque commande a un orderId unique, donc chaque cle est differente : on observe ici
# la qualite de la fonction de hachage, pas une garantie d'ordre.
$distinctKeys = ($results | Select-Object -ExpandProperty orderId | Sort-Object -Unique).Count
Write-Host "  cles distinctes : $distinctKeys sur $Count"

Write-Host "`nHigh water mark par partition (total cumule du topic) :"
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:19092 --topic $Topic

# WHY: sur Windows PowerShell 5.1, rediriger stderr d'un executable natif (2>$null) fait
# remonter chaque ligne en ErrorRecord et stoppe le script avec $ErrorActionPreference="Stop".
# Les outils Kafka ecrivent des avertissements sur stderr en fonctionnement normal.
$ErrorActionPreference = "Continue"

Write-Host "`n=== Partie B : la meme cle, cinq fois ===" -ForegroundColor Cyan
Write-Host "Topic de laboratoire $LabTopic (3 partitions), pour ne pas polluer les topics metier."

# Recree le topic a chaque execution : on veut compter les messages de CETTE execution.
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 `
    --delete --topic $LabTopic --if-exists | Out-Null
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 `
    --create --topic $LabTopic --partitions 3 --replication-factor 1 | Out-Null

# parse.key=true : le producteur console lit "cle:valeur" et envoie une vraie cle.
#
# WHY on passe par un fichier copie dans le conteneur plutot que par un pipe PowerShell :
# PowerShell 5.1 prefixe le flux envoye a un executable natif d'un BOM UTF-8. Ce BOM finit
# dans la PREMIERE cle : "order-A" devient "﻿order-A", une cle differente, donc une
# autre partition — alors que les deux s'affichent exactement pareil a l'ecran. Une cle Kafka
# est une suite d'octets, pas un texte : deux cles visuellement identiques mais encodees
# differemment partent sur deux partitions, et l'ordre entre elles est perdu.
$keysFile = Join-Path $env:TEMP "lab-keys.txt"
@(
    "order-A:1", "order-A:2", "order-A:3", "order-A:4", "order-A:5",
    "order-B:1", "order-C:1", "order-D:1"
) | Set-Content -Path $keysFile -Encoding ascii

docker cp $keysFile kafka:/tmp/lab-keys.txt | Out-Null
docker exec kafka sh -c "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:19092 --topic $LabTopic --property parse.key=true --property key.separator=: < /tmp/lab-keys.txt"

Write-Host "`nContenu de $LabTopic (cle -> partition) :"
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 `
    --topic $LabTopic --from-beginning --timeout-ms 5000 `
    --property print.key=true --property print.partition=true

Write-Host "`nLes cinq 'order-A' doivent etre sur UNE SEULE partition." -ForegroundColor Yellow
Write-Host "C'est la seule garantie d'ordre que Kafka donne : par partition, donc par cle."

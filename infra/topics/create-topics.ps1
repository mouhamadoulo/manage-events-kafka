# Creation explicite des topics du laboratoire (equivalent PowerShell de create-topics.sh).
# auto.create.topics.enable=false cote broker : rien n'existe sans passer par ici.
#
# Usage : .\infra\topics\create-topics.ps1

$ErrorActionPreference = 'Stop'

$Container = if ($env:KAFKA_CONTAINER) { $env:KAFKA_CONTAINER } else { 'kafka' }
$Bootstrap = if ($env:KAFKA_BOOTSTRAP) { $env:KAFKA_BOOTSTRAP } else { 'localhost:19092' }
$Kt = '/opt/kafka/bin/kafka-topics.sh'

$SevenDays = 604800000
$ThirtyDays = 2592000000

function New-KafkaTopic {
    param(
        [string] $Name,
        [int] $Partitions,
        [string] $Policy,
        [long] $RetentionMs = 0
    )

    $cfg = @('--config', "cleanup.policy=$Policy")
    if ($Policy -eq 'delete' -and $RetentionMs -gt 0) {
        $cfg += @('--config', "retention.ms=$RetentionMs")
    }
    if ($Policy -eq 'compact') {
        # Compaction agressive : utile pour voir l'effet rapidement en dev.
        $cfg += @('--config', 'min.cleanable.dirty.ratio=0.1', '--config', 'segment.ms=60000')
    }

    Write-Host "  -> $Name ($Partitions p, $Policy)"
    $args = @('exec', '-i', $Container, $Kt,
              '--bootstrap-server', $Bootstrap,
              '--create', '--if-not-exists',
              '--topic', $Name,
              '--partitions', $Partitions,
              '--replication-factor', '1') + $cfg
    & docker @args | Out-Null
}

Write-Host 'Topics metier :'
# Cle = orderId pour tout ce qui concerne la commande : garantit l'ordre par commande.
New-KafkaTopic 'shop.order.events.v1'     3 'delete' $SevenDays
New-KafkaTopic 'shop.payment.events.v1'   3 'delete' $SevenDays
# Cle = sku : l'ordre qui compte ici est celui des mouvements d'un article.
New-KafkaTopic 'shop.inventory.events.v1' 3 'delete' $SevenDays
New-KafkaTopic 'shop.shipping.events.v1'  3 'delete' $SevenDays

Write-Host 'Event store (phase 8) :'
# Compacte : la derniere valeur par cle est conservee indefiniment.
New-KafkaTopic 'shop.order.store.v1'      3 'compact'

Write-Host 'Retry + DLT (phase 4) :'
foreach ($base in @('shop.order.events.v1', 'shop.payment.events.v1', 'shop.inventory.events.v1', 'shop.shipping.events.v1')) {
    New-KafkaTopic "$base.retry-1" 3 'delete' $SevenDays
    New-KafkaTopic "$base.retry-2" 3 'delete' $SevenDays
    # DLT a 1 partition : volume faible, et l'ordre global y est plus lisible pour le diagnostic.
    New-KafkaTopic "$base.dlt"     1 'delete' $ThirtyDays
}

Write-Host ''
Write-Host 'Topics existants :'
& docker exec -i $Container $Kt --bootstrap-server $Bootstrap --list | Sort-Object

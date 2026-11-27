<#
  Experience P4-T6 — le message empoisonne.

  Un seul message mal forme suffisait, jusqu'a la phase 4, a bloquer une partition POUR
  TOUJOURS : la deserialisation echoue AVANT le listener, l'offset n'est donc jamais commite,
  et le meme message est repolle indefiniment. Tous les messages suivants de cette partition
  attendent derriere lui.

  Protocole, sur une SEULE partition (meme cle pour tout) :
    1. N evenements valides ;
    2. un message brut, sans l'octet magique du format Confluent, injecte par
       kafka-console-producer avec la meme cle ;
    3. N evenements valides de plus.

  Deux mesures comparees :
    - lag du groupe sur cette partition, avant et apres ;
    - nombre de messages dans le dead letter topic ;
    - compteur /stats de notification-service.

  Attendu AVEC ErrorHandlingDeserializer (defaut) :
    1 message en .dlt, 2N traites, lag revenu a 0.

  Attendu SANS (temoin, -Temoin) :
    0 message en .dlt, N traites, lag qui NE redescend PAS — les N messages publies apres le
    poison ne sont jamais consommes.

  Prerequis :
    - infra up (docker compose -f infra/docker-compose.yml up -d) ;
    - order-service lance avec --spring.profiles.active=lab (pour /lab/producer/one) ;
    - notification-service lance. Pour le temoin, le lancer avec
      VALUE_DESERIALIZER=io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer.

  Usage : ./infra/experiments/p4-t6-poison.ps1 [-Valid 10] [-Temoin]
#>
param(
    [string]$Topic = "shop.order.events.v1",
    [string]$OrderServiceUrl = "http://localhost:8091",
    [string]$NotificationUrl = "http://localhost:8095",
    [string]$Group = "notification-service",
    # Meme cle pour TOUS les messages : la cle decide de la partition, donc tout atterrit sur
    # la meme. Sans cela, les messages publies apres le poison partiraient sur d'autres
    # partitions et continueraient d'etre consommes — le blocage serait invisible.
    [string]$OrderId = "order-poison-demo",
    [int]$Valid = 10,
    [int]$WaitSeconds = 40,
    [switch]$Temoin
)

$ErrorActionPreference = "Stop"

$Container = if ($env:KAFKA_CONTAINER) { $env:KAFKA_CONTAINER } else { "kafka" }
$Bootstrap = "localhost:19092"
$DltTopic = "$Topic.dlt"
$Retry1 = "$Topic.retry-1"

# Les outils Kafka ecrivent des avertissements sur STDERR en fonctionnement normal. Sous
# $ErrorActionPreference = "Stop", chaque ligne devient un ErrorRecord et stoppe le script
# (piege documente dans CLAUDE.md §5).
function Invoke-Docker {
    param([string[]]$DockerArgs)
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $output = & docker @DockerArgs 2>&1
    $ErrorActionPreference = $previous
    return $output
}

function Get-EndOffset {
    param([string]$TopicName, [int]$Partition = -1)
    $lines = Invoke-Docker @("exec", "-i", $Container, "/opt/kafka/bin/kafka-get-offsets.sh",
        "--bootstrap-server", $Bootstrap, "--topic", $TopicName)
    $total = 0
    foreach ($line in $lines) {
        # Format : <topic>:<partition>:<offset>
        if ($line -match "^$([regex]::Escape($TopicName)):(\d+):(\d+)$") {
            if ($Partition -lt 0 -or [int]$Matches[1] -eq $Partition) {
                $total += [long]$Matches[2]
            }
        }
    }
    return $total
}

function Get-GroupState {
    $lines = Invoke-Docker @("exec", "-i", $Container, "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server", $Bootstrap, "--describe", "--group", $Group)
    $rows = @()
    foreach ($line in $lines) {
        if ($line -match "^\s*$([regex]::Escape($Group))\s+(\S+)\s+(\d+)\s+(\S+)\s+(\S+)\s+(\S+)") {
            $rows += [pscustomobject]@{
                Topic     = $Matches[1]
                Partition = [int]$Matches[2]
                Offset    = $Matches[3]
                EndOffset = $Matches[4]
                Lag       = $Matches[5]
            }
        }
    }
    return $rows
}

function Show-GroupState {
    param([string]$Label)
    Write-Host ""
    Write-Host "--- $Label" -ForegroundColor Cyan
    $rows = Get-GroupState
    if (-not $rows) {
        Write-Host "    (aucune partition assignee : le groupe est inactif)"
        return
    }
    $rows | Where-Object { $_.Topic -eq $Topic } |
        Sort-Object Partition |
        Format-Table Topic, Partition, Offset, EndOffset, Lag -AutoSize |
        Out-String | Write-Host
}

function Get-Stats {
    try {
        return Invoke-RestMethod -Uri "$NotificationUrl/stats" -Method Get -TimeoutSec 5
    } catch {
        throw "notification-service injoignable sur $NotificationUrl : $($_.Exception.Message)"
    }
}

function Publish-Valid {
    param([int]$Count)
    for ($i = 0; $i -lt $Count; $i++) {
        $uri = "$OrderServiceUrl/lab/producer/one?orderId=$OrderId"
        $result = Invoke-RestMethod -Uri $uri -Method Post -TimeoutSec 15
        Write-Host ("    valide #{0} partition={1} offset={2} eventId={3}" -f `
            ($i + 1), $result.partition, $result.offset, $result.eventId)
    }
}

function Publish-Poison {
    # Un JSON parfaitement valide, mais SANS l'octet magique 0 ni l'id de schema sur 4 octets
    # que le format Confluent impose. Le deserialiseur lit donc le premier caractere comme un
    # octet magique et echoue : « Unknown magic byte! ».
    $payload = '{"envelope":{"eventId":"00000000-0000-0000-0000-000000000000"},"orderId":"' + $OrderId + '"}'
    $line = "$OrderId`t$payload"

    # PowerShell 5.1 ajoute un BOM UTF-8 quand on pipe vers un executable natif : la cle
    # deviendrait "<BOM>order-poison-demo", qui part sur une AUTRE partition pour un affichage
    # identique (piege documente dans CLAUDE.md §5). On passe donc par un fichier.
    $local = Join-Path $env:TEMP "p4-t6-poison.txt"
    Set-Content -Path $local -Value $line -Encoding ascii -NoNewline
    Add-Content -Path $local -Value "`n" -Encoding ascii -NoNewline

    Invoke-Docker @("cp", $local, "${Container}:/tmp/poison.txt") | Out-Null
    $produce = "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $Bootstrap " +
               "--topic $Topic --property parse.key=true --property key.separator=`$'\t' " +
               "< /tmp/poison.txt"
    Invoke-Docker @("exec", "-i", $Container, "sh", "-c", $produce) | Out-Null
    Write-Host "    poison injecte sur la cle $OrderId" -ForegroundColor Yellow
}

Write-Host ""
if ($Temoin) {
    Write-Host "=== P4-T6 — TEMOIN : consumer SANS ErrorHandlingDeserializer ===" -ForegroundColor Red
} else {
    Write-Host "=== P4-T6 — message empoisonne, consumer protege ===" -ForegroundColor Green
}

$statsBefore = Get-Stats
$dltBefore = Get-EndOffset $DltTopic
$retry1Before = Get-EndOffset $Retry1
Write-Host ""
Write-Host "Etat initial : recus=$($statsBefore.received) dlt=$dltBefore retry-1=$retry1Before"
Show-GroupState "lag avant"

Write-Host ""
Write-Host "1/3 — $Valid evenements valides sur la cle $OrderId"
Publish-Valid -Count $Valid

Write-Host ""
Write-Host "2/3 — injection du message hors format"
Publish-Poison

Write-Host ""
Write-Host "3/3 — $Valid evenements valides de plus, meme cle donc meme partition"
Publish-Valid -Count $Valid

Write-Host ""
Write-Host "Attente de $WaitSeconds s pour laisser le consumer rattraper..."
$deadline = (Get-Date).AddSeconds($WaitSeconds)
$expected = $statsBefore.received + (2 * $Valid)
while ((Get-Date) -lt $deadline) {
    $current = (Get-Stats).received
    if ($current -ge $expected) { break }
    Start-Sleep -Milliseconds 1000
}

$statsAfter = Get-Stats
$dltAfter = Get-EndOffset $DltTopic
$retry1After = Get-EndOffset $Retry1
Show-GroupState "lag apres"

$processed = $statsAfter.received - $statsBefore.received
$inDlt = $dltAfter - $dltBefore
$inRetry = $retry1After - $retry1Before
# Le lag vaut "-" quand le groupe n'a pas de proprietaire — ce qui arrive juste apres un
# redemarrage, donc pendant le temoin. Measure-Object sur ces chaines leve sous
# $ErrorActionPreference = "Stop" : on convertit explicitement, en ignorant les non-numeriques.
$poisonedLag = 0
foreach ($row in (Get-GroupState | Where-Object { $_.Topic -eq $Topic })) {
    $valeur = 0
    if ([int]::TryParse($row.Lag, [ref]$valeur)) { $poisonedLag += $valeur }
}

Write-Host ""
Write-Host "=== Resultat ===" -ForegroundColor Cyan
Write-Host ("  valides publies ........ {0}" -f (2 * $Valid))
Write-Host ("  traites par le consumer  {0}" -f $processed)
Write-Host ("  messages en .dlt ....... {0}" -f $inDlt)
Write-Host ("  messages en .retry-1 ... {0}" -f $inRetry)
Write-Host ("  lag total sur $Topic ... {0}" -f $poisonedLag)
Write-Host ""

if ($Temoin) {
    if ($processed -lt (2 * $Valid) -and $poisonedLag -gt 0) {
        Write-Host "CONFORME au temoin : la partition est bloquee, $processed messages sur $(2 * $Valid) traites." -ForegroundColor Red
        Write-Host "Le message hors format est repolle indefiniment et rien ne passe derriere lui."
    } else {
        Write-Host "INATTENDU : le temoin devrait rester bloque. Le consumer tourne-t-il bien sans ErrorHandlingDeserializer ?" -ForegroundColor Yellow
    }
} else {
    if ($processed -eq (2 * $Valid) -and $inDlt -eq 1 -and $poisonedLag -eq 0) {
        Write-Host "CONFORME : 1 message en .dlt, $processed valides traites, lag a 0." -ForegroundColor Green
        Write-Host "Le message hors format n'a jamais atteint le listener, et il n'a bloque personne."
    } else {
        Write-Host "INATTENDU : attendu $(2 * $Valid) traites, 1 en dlt, lag 0." -ForegroundColor Yellow
    }
    Write-Host ""
    Write-Host "Note : .retry-1 doit rester a 0. Un echec de deserialisation est NON-RETRYABLE —"
    Write-Host "un message qui ne se decode pas ne se decodera pas mieux dans 5 secondes."
}
Write-Host ""

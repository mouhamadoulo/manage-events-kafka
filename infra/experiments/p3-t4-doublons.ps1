<#
  Experience P3-T4 — pourquoi enable.idempotence existe.

  Le scenario qui duplique est toujours le meme : le broker ECRIT le record, puis l'accuse
  n'arrive pas a temps. Le producteur, qui n'a rien recu, reessaye. Sans idempotence, le
  broker n'a aucun moyen de savoir que c'est le meme record : il l'ecrit une seconde fois.

  Pour le provoquer a coup sur, on ne casse rien : on met simplement request.timeout.ms a
  5 ms, en dessous du temps de reponse normal du broker. Chaque requete est donc declaree
  expiree alors que le broker l'a traitee, et le producteur reessaye.

  WHY pas "docker pause" : essaye d'abord, et sans resultat. Geler le broker l'empeche aussi
  bien d'ECRIRE que d'ACCUSER — il reprend ou il en etait et accuse normalement. Le doublon
  demande que l'ecriture reussisse ET que l'accuse soit perdu, ce qu'un timeout client
  reproduit exactement, et de facon deterministe.

  Le comptage se fait sur les eventId : chaque evenement en porte un, genere une seule fois.
  Deux occurrences du meme eventId = le meme record ecrit deux fois.

  ATTENTION : ce script purge shop.order.events.v1.

  Prerequis : infra up, order-service lance avec --spring.profiles.active=lab.
  Usage     : ./infra/experiments/p3-t4-doublons.ps1
#>
param(
    [int]$Count = 20000,
    [int]$RequestTimeoutMs = 5,
    [string]$Topic = "shop.order.events.v1",
    [string]$OrderServiceUrl = "http://localhost:8091"
)

$ErrorActionPreference = "Stop"

function Invoke-Docker {
    param([string[]]$DockerArgs)
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $output = & docker @DockerArgs 2>&1
    $ErrorActionPreference = $previous
    return $output
}

function Reset-Topic {
    Invoke-Docker @("exec", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server",
        "localhost:19092", "--delete", "--topic", $Topic) | Out-Null
    Start-Sleep -Seconds 5
    Invoke-Docker @("exec", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server",
        "localhost:19092", "--create", "--if-not-exists", "--topic", $Topic,
        "--partitions", "3", "--replication-factor", "1",
        "--config", "cleanup.policy=delete", "--config", "retention.ms=604800000") | Out-Null
}

function Get-StoredCount {
    $lines = Invoke-Docker @("exec", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
        "--bootstrap-server", "localhost:19092", "--topic", $Topic)
    $total = 0
    foreach ($line in $lines) { if ($line -match ":(\d+)$") { $total += [int]$Matches[1] } }
    return $total
}

function Measure-Duplicates {
    param([int]$Expected)
    if ($Expected -le 0) { return [pscustomobject]@{ Lus = 0; Distincts = 0; Doublons = 0 } }

    $lines = Invoke-Docker @("exec", "kafka", "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server", "localhost:19092", "--topic", $Topic, "--from-beginning",
        "--max-messages", "$Expected", "--timeout-ms", "120000")
    $ids = $lines | Select-String -Pattern '"eventId":"([^"]+)"' -AllMatches |
        ForEach-Object { $_.Matches } | ForEach-Object { $_.Groups[1].Value }
    $distinct = ($ids | Sort-Object -Unique).Count
    return [pscustomobject]@{ Lus = $ids.Count; Distincts = $distinct; Doublons = $ids.Count - $distinct }
}

function Test-Idempotence {
    param([bool]$Idempotence)

    $label = if ($Idempotence) { "enable.idempotence=true" } else { "enable.idempotence=false" }
    Write-Host "`n=== $label ===" -ForegroundColor Cyan
    Reset-Topic

    $uri = "$OrderServiceUrl/lab/producer/burst?count=$Count&acks=all" +
           "&idempotence=$($Idempotence.ToString().ToLower())" +
           "&lingerMs=0&compression=none&requestTimeoutMs=$RequestTimeoutMs" +
           "&deliveryTimeoutMs=60000&maxBlockMs=20000&abortOnFailure=false"

    $result = Invoke-RestMethod -Method Post -Uri $uri -TimeoutSec 900

    Write-Host "  le producteur declare : $($result.acknowledged) envoyes, $($result.failed) echecs en $($result.durationMs) ms"

    $stored = Get-StoredCount
    $analysis = Measure-Duplicates -Expected $stored
    Write-Host "  le topic contient     : $stored records, $($analysis.Distincts) eventId distincts"
    if ($analysis.Doublons -gt 0) {
        $taux = [math]::Round(100 * $analysis.Doublons / $analysis.Distincts, 1)
        Write-Host "  DOUBLONS : $($analysis.Doublons) records ecrits en trop ($taux % de plus)" -ForegroundColor Red
    } else {
        Write-Host "  aucun doublon" -ForegroundColor Green
    }

    return [pscustomobject]@{
        Configuration = $label
        Accuses = [int]$result.acknowledged
        DureeMs = [int]$result.durationMs
        Stockes = $stored
        Distincts = $analysis.Distincts
        Doublons = $analysis.Doublons
    }
}

$resultats = @()
$resultats += Test-Idempotence -Idempotence $false
$resultats += Test-Idempotence -Idempotence $true

Write-Host "`n=== Bilan ===" -ForegroundColor Cyan
Write-Host ("  {0,-26} {1,9} {2,9} {3,11} {4,9} {5,9}" -f "configuration", "accuses", "stockes", "distincts", "doublons", "duree")
foreach ($r in $resultats) {
    Write-Host ("  {0,-26} {1,9} {2,9} {3,11} {4,9} {5,8}ms" -f `
        $r.Configuration, $r.Accuses, $r.Stockes, $r.Distincts, $r.Doublons, $r.DureeMs)
}

Write-Host "`nA noter : les deux durees sont equivalentes. Le cout des doublons ne se voit" -ForegroundColor Yellow
Write-Host "pas dans le temps d'envoi mais en aval — espace disque, bande passante, et surtout"
Write-Host "chaque consumer qui retraite deux fois le meme fait metier."
Write-Host "`nCe que l'idempotence ne couvre PAS : elle vaut pour un meme producteur, dans une"
Write-Host "meme session. Un service qui republie apres un redemarrage, ou un consumer qui"
Write-Host "retraite un message, dupliquent malgre elle. D'ou l'idempotence cote consumer"
Write-Host "(phase 4) et les transactions (phase 7)."

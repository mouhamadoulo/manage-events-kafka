<#
  Experience P1-T6 — offsets : rejeu complet, puis rejeu partiel dans le temps.

  Ce que l'experience doit rendre evident :
    1. l'offset appartient au GROUPE, pas au topic : le rejouer ne touche pas les messages ;
    2. on ne peut pas deplacer les offsets d'un groupe qui a des membres actifs ;
    3. apres --to-earliest, le consumer retraite TOUT. Sans idempotence, chaque effet de
       bord est rejoue — c'est pour cela que la phase 4 ajoute processed_event.

  Prerequis : infra up, order-service sur 8091, jar notification-service construit.
  Usage     : ./infra/experiments/p1-t6-offsets.ps1
#>
param(
    [string]$Group = "notification-service",
    [string]$Topic = "shop.order.events.v1",
    [int]$Port = 8095,
    [int]$MinutesBack = 10,
    [int]$Orders = 8,
    [string]$OrderServiceUrl = "http://localhost:8091"
)

$ErrorActionPreference = "Stop"
$jar = "notification-service/target/notification-service-0.1.0-SNAPSHOT.jar"
$java = Join-Path $env:JAVA_HOME "bin/java.exe"
if (-not (Test-Path $jar)) { throw "jar absent : $jar" }
if (-not (Test-Path $java)) { throw "JAVA_HOME ne pointe pas sur un JDK : $env:JAVA_HOME" }

function Invoke-Kafka {
    param([string[]]$KafkaArgs)
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh `
        --bootstrap-server localhost:19092 @KafkaArgs 2>&1 |
        Where-Object { $_ -notmatch "KIP-848|Consider setting" }
    $ErrorActionPreference = $previous
}

function Start-Consumer {
    $log = Join-Path $env:TEMP "notif-$Port.log"
    $process = Start-Process -FilePath $java `
        -ArgumentList @("-jar", $jar, "--server.port=$Port") `
        -RedirectStandardOutput $log -RedirectStandardError "$log.err" `
        -PassThru -WindowStyle Hidden
    for ($i = 0; $i -lt 90; $i++) {
        try {
            if ((Invoke-RestMethod "http://localhost:$Port/actuator/health" -TimeoutSec 2).status -eq "UP") {
                return $process
            }
        } catch { Start-Sleep -Seconds 1 }
    }
    throw "le consumer ne demarre pas"
}

function Stop-Consumer {
    param($Process)
    if ($Process -and -not $Process.HasExited) { Stop-Process -Id $Process.Id -Force -Confirm:$false }

    # Un process tue de force n'envoie pas de LeaveGroup : le coordinateur continue de
    # compter le membre jusqu'a expiration de session.timeout.ms (45 s par defaut en
    # Kafka 4). D'ici la, le groupe reste "Stable" et tout reset d'offset est refuse.
    # C'est aussi la raison pour laquelle les partitions d'un consumer qui crashe ne sont
    # pas reattribuees immediatement — a la difference d'un arret propre.
    $chrono = [System.Diagnostics.Stopwatch]::StartNew()
    for ($i = 0; $i -lt 60; $i++) {
        $state = Invoke-Kafka @("--describe", "--group", $Group, "--state")
        if ($state -match "\bEmpty\b") {
            Write-Host ("groupe vide apres {0:N0} s (expiration de session.timeout.ms)" -f $chrono.Elapsed.TotalSeconds)
            return
        }
        Start-Sleep -Seconds 2
    }
    throw "le groupe $Group est toujours actif apres 120 s"
}

$consumer = $null
try {
    Write-Host "=== 1. Etat de depart ===" -ForegroundColor Cyan
    $consumer = Start-Consumer
    Start-Sleep -Seconds 4

    Write-Host "$Orders commandes, pour que le groupe ait quelque chose de frais a traiter"
    foreach ($i in 1..$Orders) {
        $body = @{
            customerId = "customer-$i"
            lines      = @(@{ sku = "SKU-1"; quantity = 1; unitPrice = 3.20 })
        } | ConvertTo-Json -Compress
        Invoke-RestMethod -Uri "$OrderServiceUrl/orders" -Method Post `
            -ContentType 'application/json' -Body $body | Out-Null
    }
    Start-Sleep -Seconds 4
    $before = Invoke-RestMethod "http://localhost:$Port/stats"
    Write-Host "l'instance a traite $($before.received) evenement(s) depuis son demarrage"
    Invoke-Kafka @("--describe", "--group", $Group)

    Write-Host "`n=== 2. Tentative de reset avec un groupe actif ===" -ForegroundColor Cyan
    Write-Host "Attendu : refus. Deplacer les offsets d'un groupe en cours de consommation"
    Write-Host "reviendrait a changer sa position pendant qu'il lit."
    Invoke-Kafka @("--reset-offsets", "--group", $Group, "--topic", $Topic, "--to-earliest", "--execute")

    Write-Host "`n=== 3. Arret du consumer, puis rejeu complet ===" -ForegroundColor Cyan
    Stop-Consumer -Process $consumer
    $consumer = $null

    Write-Host "--dry-run d'abord : ce que le reset FERAIT." -ForegroundColor Yellow
    Invoke-Kafka @("--reset-offsets", "--group", $Group, "--topic", $Topic, "--to-earliest", "--dry-run")

    Write-Host "`npuis --execute :"
    Invoke-Kafka @("--reset-offsets", "--group", $Group, "--topic", $Topic, "--to-earliest", "--execute")

    Write-Host "`nLe LAG est maintenant egal au contenu complet du topic :"
    Invoke-Kafka @("--describe", "--group", $Group)

    Write-Host "`nRedemarrage : le consumer retraite tout." -ForegroundColor Yellow
    $consumer = Start-Consumer
    Start-Sleep -Seconds 10
    $replay = Invoke-RestMethod "http://localhost:$Port/stats"
    $detail = ($replay.byPartition.PSObject.Properties | ForEach-Object { "p$($_.Name)=$($_.Value)" }) -join " "
    Write-Host "rejeu complet : $($replay.received) evenements   $detail"
    Write-Host "Aucun message n'a ete republie : seul l'offset du groupe a bouge." -ForegroundColor Yellow

    Write-Host "`n=== 4. Rejeu partiel : --to-datetime ===" -ForegroundColor Cyan
    Stop-Consumer -Process $consumer
    $consumer = $null

    # Format attendu : yyyy-MM-ddTHH:mm:ss.SSS, avec decalage explicite pour lever l'ambiguite
    # de fuseau. Kafka cherche, dans chaque partition, le premier offset dont le timestamp
    # est >= a cette date.
    $since = (Get-Date).ToUniversalTime().AddMinutes(-$MinutesBack).ToString("yyyy-MM-ddTHH:mm:ss.fff") + "+00:00"
    Write-Host "reset a $since (UTC), soit les $MinutesBack dernieres minutes"
    Invoke-Kafka @("--reset-offsets", "--group", $Group, "--topic", $Topic, "--to-datetime", $since, "--execute")
    Invoke-Kafka @("--describe", "--group", $Group)

    $consumer = Start-Consumer
    Start-Sleep -Seconds 10
    $partial = Invoke-RestMethod "http://localhost:$Port/stats"
    Write-Host "`nrejeu partiel : $($partial.received) evenements (contre $($replay.received) en complet)"
}
finally {
    Write-Host "`n>>> arret du consumer" -ForegroundColor Green
    if ($consumer -and -not $consumer.HasExited) { Stop-Process -Id $consumer.Id -Force -Confirm:$false }
}

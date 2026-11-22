<#
  Experience P1-T7 — @KafkaListener contre binding fonctionnel Spring Cloud Stream.

  Le meme module, le meme topic, le meme evenement, deux modeles de programmation.
  On lance les deux a la suite (jamais en meme temps : deux groupes distincts compteraient
  chaque evenement deux fois) et on compare ce qui est consomme.

  La conclusion et la decision par module sont dans docs/adr/ADR-002.

  Prerequis : infra up, order-service sur 8091, jar notification-service construit.
  Usage     : ./infra/experiments/p1-t7-deux-modeles.ps1 [-Orders 5]
#>
param(
    [int]$Orders = 5,
    [int]$Port = 8095,
    [string]$OrderServiceUrl = "http://localhost:8091"
)

$ErrorActionPreference = "Stop"
$jar = "notification-service/target/notification-service-0.1.0-SNAPSHOT.jar"
$java = Join-Path $env:JAVA_HOME "bin/java.exe"
if (-not (Test-Path $jar)) { throw "jar absent : $jar" }
if (-not (Test-Path $java)) { throw "JAVA_HOME ne pointe pas sur un JDK : $env:JAVA_HOME" }

function Measure-Model {
    param([string]$Label, [string[]]$ExtraArgs)

    Write-Host "`n=== $Label ===" -ForegroundColor Cyan
    $log = Join-Path $env:TEMP "notif-$Label.log"
    $chrono = [System.Diagnostics.Stopwatch]::StartNew()
    $process = Start-Process -FilePath $java `
        -ArgumentList (@("-jar", $jar, "--server.port=$Port") + $ExtraArgs) `
        -RedirectStandardOutput $log -RedirectStandardError "$log.err" `
        -PassThru -WindowStyle Hidden
    try {
        for ($i = 0; $i -lt 90; $i++) {
            try {
                if ((Invoke-RestMethod "http://localhost:$Port/actuator/health" -TimeoutSec 2).status -eq "UP") { break }
            } catch { Start-Sleep -Seconds 1 }
        }
        $startup = $chrono.Elapsed.TotalSeconds
        Start-Sleep -Seconds 6
        Invoke-RestMethod "http://localhost:$Port/stats/reset" | Out-Null

        foreach ($i in 1..$Orders) {
            $body = @{
                customerId = "customer-$i"
                lines      = @(@{ sku = "SKU-7"; quantity = 2; unitPrice = 6.25 })
            } | ConvertTo-Json -Compress
            Invoke-RestMethod -Uri "$OrderServiceUrl/orders" -Method Post `
                -ContentType 'application/json' -Body $body | Out-Null
        }
        Start-Sleep -Seconds 5

        $stats = Invoke-RestMethod "http://localhost:$Port/stats"
        $detail = ($stats.byPartition.PSObject.Properties | ForEach-Object { "p$($_.Name)=$($_.Value)" }) -join " "
        Write-Host ("  modele          : {0}" -f $stats.model)
        Write-Host ("  groupe          : {0}" -f $stats.groupId)
        Write-Host ("  demarrage       : {0:N1} s jusqu'a UP" -f $startup)
        Write-Host ("  consomme        : {0} evenements sur {1} publies   {2}" -f $stats.received, $Orders, $detail)

        $lines = Get-Content $log
        Write-Host ("  lignes de log   : {0}" -f $lines.Count)
        Write-Host "  exemple :"
        $lines | Select-String -Pattern "recu" | Select-Object -Last 1 |
            ForEach-Object { Write-Host "    $($_.Line.Trim())" }
    }
    finally {
        if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force -Confirm:$false }
        # Attendre l'expiration de session.timeout.ms serait inutile ici : les deux variantes
        # utilisent des groupes differents, elles ne se genent pas.
        Start-Sleep -Seconds 3
    }
}

Measure-Model -Label "spring-kafka" -ExtraArgs @()
Measure-Model -Label "cloud-stream" -ExtraArgs @("--spring.profiles.active=cloud-stream")

Write-Host "`nLes deux consomment la meme chose. La difference est ailleurs :" -ForegroundColor Yellow
Write-Host "  - ce que le code doit connaitre de Kafka ;"
Write-Host "  - ou se fait la conversion du payload (Deserializer Kafka / MessageConverter) ;"
Write-Host "  - ce qu'on peut encore faire quand on sort de ce que le binder expose."
Write-Host "Decision par module : docs/adr/ADR-002-spring-kafka-vs-cloud-stream.md"

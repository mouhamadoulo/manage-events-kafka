<#
  Experience P1-T5 — consumer group et rebalancing.

  On lance les instances une par une dans LE MEME groupe et on regarde, apres chaque
  arrivee, qui possede quelle partition. Le topic a 3 partitions ; la 4e instance est
  volontaire : elle montre qu'un consumer de plus qu'il n'y a de partitions ne sert a rien.

  Prerequis : infra up, order-service lance sur 8091, jar notification-service construit
              (./mvnw -pl notification-service -am -DskipTests package).
  Usage     : ./infra/experiments/p1-t5-rebalancing.ps1 [-Instances 4] [-Orders 30]
#>
param(
    [int]$Instances = 4,
    [int]$Orders = 30,
    [string]$Group = "notification-service",
    [string]$OrderServiceUrl = "http://localhost:8091",
    [int]$FirstPort = 8095
)

$ErrorActionPreference = "Stop"
$jar = "notification-service/target/notification-service-0.1.0-SNAPSHOT.jar"
if (-not (Test-Path $jar)) { throw "jar absent : $jar" }
$java = Join-Path $env:JAVA_HOME "bin/java.exe"
if (-not (Test-Path $java)) { throw "JAVA_HOME ne pointe pas sur un JDK : $env:JAVA_HOME" }

function Show-Group {
    param([string]$Label)
    Write-Host "`n--- $Label ---" -ForegroundColor Cyan
    # ErrorActionPreference relache : les outils Kafka ecrivent sur stderr en fonctionnement
    # normal, et PowerShell 5.1 transforme chaque ligne en ErrorRecord.
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh `
        --bootstrap-server localhost:19092 --describe --group $Group 2>&1 |
        Where-Object { $_ -notmatch "KIP-848|Consider setting" }
    $ErrorActionPreference = $previous
}

function Wait-Health {
    param([int]$Port)
    for ($i = 0; $i -lt 90; $i++) {
        try {
            $health = Invoke-RestMethod "http://localhost:$Port/actuator/health" -TimeoutSec 2
            if ($health.status -eq "UP") { return }
        } catch { Start-Sleep -Seconds 1 }
    }
    throw "instance $Port ne demarre pas"
}

$processes = @()
$ports = @()

try {
    foreach ($n in 1..$Instances) {
        # Pas de 100 : 8095, 8195, 8295... pour ne pas marcher sur les ports des autres
        # services du labo (payment 8092, analytics 8096, cf. CLAUDE.md §3).
        $port = $FirstPort + ($n - 1) * 100
        $ports += $port
        $log = Join-Path $env:TEMP "notif-$port.log"

        Write-Host "`n>>> demarrage instance $n sur le port $port (groupe $Group)" -ForegroundColor Green
        $processes += Start-Process -FilePath $java `
            -ArgumentList @("-jar", $jar, "--server.port=$port") `
            -RedirectStandardOutput $log -RedirectStandardError "$log.err" `
            -PassThru -WindowStyle Hidden
        Wait-Health -Port $port

        # Le rebalancing n'est pas instantane : le nouveau membre doit rejoindre le groupe
        # et le coordinateur redistribuer les partitions. Sans cette pause, --describe
        # montre un etat intermediaire.
        Start-Sleep -Seconds 6
        Show-Group -Label "$n instance(s) dans le groupe"
    }

    Write-Host "`n>>> $Orders commandes pour voir qui traite quoi" -ForegroundColor Green
    foreach ($i in 1..$Orders) {
        $body = @{
            customerId = "customer-$($i % 7)"
            lines      = @(@{ sku = "SKU-$($i % 3)"; quantity = 1; unitPrice = 4.50 })
        } | ConvertTo-Json -Compress
        Invoke-RestMethod -Uri "$OrderServiceUrl/orders" -Method Post `
            -ContentType 'application/json' -Body $body | Out-Null
    }
    Start-Sleep -Seconds 3

    Write-Host "`nTraitement par instance :" -ForegroundColor Cyan
    foreach ($port in $ports) {
        $stats = Invoke-RestMethod "http://localhost:$port/stats"
        $detail = ($stats.byPartition.PSObject.Properties |
            ForEach-Object { "p$($_.Name)=$($_.Value)" }) -join " "
        if (-not $detail) { $detail = "(aucune partition assignee)" }
        Write-Host ("  port {0} : {1,4} evenements   {2}" -f $port, $stats.received, $detail)
    }

    Show-Group -Label "etat final"
}
finally {
    Write-Host "`n>>> arret des instances" -ForegroundColor Green
    foreach ($p in $processes) {
        if (-not $p.HasExited) { Stop-Process -Id $p.Id -Force -Confirm:$false }
    }
}

<#
  Experience P3-T3 — ce que acks=0 ne dit pas.

  Protocole : on envoie une rafale, on TUE le broker au milieu, on le redemarre, et on compare
  deux nombres :
    - ce que le producteur declare avoir envoye avec succes ;
    - ce que le topic contient reellement.

  Avec acks=0, le producteur considere un record envoye des que les octets partent sur la
  socket : il n'attend aucune confirmation. L'ecart entre les deux nombres est donc une perte
  silencieuse — aucune exception, aucun log d'erreur, rien a surveiller.

  Avec acks=all, le producteur attend l'accuse du broker. Il ne peut plus se tromper sur ce
  qu'il a reussi : les envois non confirmes sont comptes en echecs, et c'est au code de les
  traiter (cf. KafkaEventPublisher).

  ATTENTION : ce script arrete et redemarre le conteneur kafka. Le topic
  shop.order.events.v1 est purge a chaque phase.

  Prerequis : infra up, order-service lance avec --spring.profiles.active=lab.
  Usage     : ./infra/experiments/p3-t3-perte.ps1 [-Count 50000]
#>
param(
    # WHY une rafale aussi longue : Start-Job demarre un processus PowerShell neuf, ce qui
    # prend une a deux secondes. Avec une rafale de 20 000 messages (environ une seconde a
    # 20 000 msg/s), le broker etait tue AVANT le premier envoi — l'experience mesurait le
    # demarrage de PowerShell. Il faut que la rafale dure assez pour etre interrompue.
    [int]$Count = 200000,
    [int]$KillAfterMs = 4000,
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
    # La somme des high water marks : le nombre de records reellement dans le topic.
    $lines = Invoke-Docker @("exec", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
        "--bootstrap-server", "localhost:19092", "--topic", $Topic)
    $total = 0
    foreach ($line in $lines) {
        if ($line -match ":(\d+)$") { $total += [int]$Matches[1] }
    }
    return $total
}

function Wait-Infra {
    for ($i = 0; $i -lt 90; $i++) {
        $status = Invoke-Docker @("inspect", "-f", "{{.State.Health.Status}}", "kafka")
        if ($status -match "healthy") { break }
        Start-Sleep -Seconds 2
    }
    # Le Schema Registry doit repondre : chaque rafale construit un producteur neuf, qui
    # doit resoudre le schema avant d'envoyer quoi que ce soit.
    for ($i = 0; $i -lt 90; $i++) {
        try {
            Invoke-RestMethod "http://localhost:8081/subjects" -TimeoutSec 3 | Out-Null
            return
        } catch { Start-Sleep -Seconds 2 }
    }
    throw "le Schema Registry ne repond pas"
}

function Test-Acks {
    param([string]$Acks)

    Write-Host "`n=== acks=$Acks ===" -ForegroundColor Cyan
    Wait-Infra
    Reset-Topic

    $uri = "$OrderServiceUrl/lab/producer/burst?count=$Count&acks=$Acks&idempotence=$(if ($Acks -eq '0') { 'false' } else { 'true' })&lingerMs=0&compression=none&deliveryTimeoutMs=10000&requestTimeoutMs=4000&maxBlockMs=1000"

    # La rafale part dans un autre processus : il faut pouvoir tuer le broker PENDANT.
    $job = Start-Job -ScriptBlock {
        param($u)
        try { Invoke-RestMethod -Method Post -Uri $u -TimeoutSec 600 }
        catch { [pscustomobject]@{ requested = -1; acknowledged = -1; failed = -1; firstFailure = $_.Exception.Message } }
    } -ArgumentList $uri

    Start-Sleep -Milliseconds $KillAfterMs
    Write-Host "  kill du broker en pleine rafale" -ForegroundColor Yellow
    Invoke-Docker @("kill", "kafka") | Out-Null

    $result = Receive-Job -Job $job -Wait
    Remove-Job -Job $job -Force

    Write-Host "  envois tentes avant arret : $($result.enqueued) sur $($result.requested) demandes"
    Write-Host "  le producteur declare     : $($result.acknowledged) envoyes, $($result.failed) echecs"
    if ($result.firstFailure) { Write-Host "  premiere erreur vue : $($result.firstFailure)" }

    Write-Host "  redemarrage du broker..."
    Invoke-Docker @("start", "kafka") | Out-Null
    Wait-Infra

    $stored = Get-StoredCount
    $lost = [int]$result.acknowledged - $stored
    Write-Host "  le topic contient  : $stored records"
    if ($lost -gt 0) {
        Write-Host "  PERTE SILENCIEUSE : $lost records declares envoyes, absents du topic" -ForegroundColor Red
    } elseif ($lost -lt 0) {
        Write-Host "  $([math]::Abs($lost)) records de plus que d'accuses (ecrits mais accuse perdu)" -ForegroundColor Yellow
    } else {
        Write-Host "  aucun ecart : ce que le producteur declare correspond au topic" -ForegroundColor Green
    }

    return [pscustomobject]@{
        Acks = $Acks
        Declares = [int]$result.acknowledged
        Echecs = [int]$result.failed
        Stockes = $stored
        Ecart = $lost
    }
}

$resultats = @()
$resultats += Test-Acks -Acks "0"
$resultats += Test-Acks -Acks "all"

Write-Host "`n=== Bilan ===" -ForegroundColor Cyan
Write-Host ("  {0,-6} {1,12} {2,8} {3,10} {4,8}" -f "acks", "declares", "echecs", "stockes", "ecart")
foreach ($r in $resultats) {
    Write-Host ("  {0,-6} {1,12} {2,8} {3,10} {4,8}" -f $r.Acks, $r.Declares, $r.Echecs, $r.Stockes, $r.Ecart)
}

Write-Host "`nCe que l'experience ne montre PAS : acks=all protege contre la perte cote broker" -ForegroundColor Yellow
Write-Host "seulement s'il y a plusieurs replicas. Sur ce labo mono-noeud, il garantit"
Write-Host "uniquement que le producteur SAIT ce qui a reussi. C'est deja l'essentiel :"
Write-Host "un echec connu est rattrapable, une perte silencieuse ne l'est pas."

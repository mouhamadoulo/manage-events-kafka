<#
  Experience P3-T5 — linger.ms, batch.size, compression.type : qu'est-ce qui limite vraiment ?

  La question n'est pas "quelle configuration est la meilleure" mais "ou passe le temps".
  Regler linger.ms quand le facteur limitant est ailleurs ne change rien, et c'est le genre
  de reglage qu'on copie sans mesurer.

  Prerequis : infra up, order-service lance avec --spring.profiles.active=lab.
  Usage     : ./infra/experiments/p3-t5-debit.ps1 [-Count 2000]
#>
param(
    [int]$Count = 3000,
    # WHY des repetitions : une rafale unique varie de plus de 50 % d'un essai a l'autre
    # (GC, ordonnancement, cache du systeme de fichiers). Conclure d'une seule mesure que
    # tel reglage est meilleur est la facon la plus courante de se tromper en tuning.
    [int]$Repeat = 3,
    [string]$OrderServiceUrl = "http://localhost:8091"
)

function Get-Median {
    param([int[]]$Values)
    $sorted = $Values | Sort-Object
    return $sorted[[math]::Floor($sorted.Count / 2)]
}

$ErrorActionPreference = "Stop"

$scenarios = @(
    @{ Nom = "defaut Kafka (linger=0, sans compression)"; Query = "lingerMs=0&batchSize=16384&compression=none" },
    @{ Nom = "linger=5ms, batch=32k, sans compression";   Query = "lingerMs=5&batchSize=32768&compression=none" },
    @{ Nom = "linger=20ms, batch=256k, sans compression"; Query = "lingerMs=20&batchSize=262144&compression=none" },
    @{ Nom = "linger=20ms, batch=256k, lz4";              Query = "lingerMs=20&batchSize=262144&compression=lz4" },
    @{ Nom = "linger=20ms, batch=256k, zstd";             Query = "lingerMs=20&batchSize=262144&compression=zstd" },
    @{ Nom = "idem + validation du schema COUPEE";        Query = "lingerMs=20&batchSize=262144&compression=zstd&validate=false" }
)

# WHY un echauffement : la premiere rafale paie le JIT, le chargement des classes, la
# resolution du schema aupres du registre et l'ouverture des connexions. Sans cela, le
# premier scenario mesure la JVM qui demarre, pas la configuration — et comme c'est le
# scenario de reference, tous les ecarts sont faux.
Write-Host "echauffement..." -ForegroundColor DarkGray
Invoke-RestMethod -Method Post -TimeoutSec 600 `
    -Uri "$OrderServiceUrl/lab/producer/burst?count=$Count&lingerMs=5&compression=none" | Out-Null

Write-Host "`n=== $Count messages par scenario, $Repeat essais ===" -ForegroundColor Cyan
Write-Host ""

$results = foreach ($scenario in $scenarios) {
    $uri = "$OrderServiceUrl/lab/producer/burst?count=$Count&$($scenario.Query)"
    $rates = @()
    $failures = 0
    foreach ($run in 1..$Repeat) {
        $r = Invoke-RestMethod -Method Post -Uri $uri -TimeoutSec 600
        $rates += [int]$r.messagesPerSecond
        $failures += $r.failed
    }
    $median = Get-Median -Values $rates
    Write-Host ("  {0,-42} {1,7} msg/s (median)   essais: {2}   echecs={3}" -f `
        $scenario.Nom, $median, ($rates -join " / "), $failures)
    [pscustomobject]@{
        Scenario = $scenario.Nom
        MsgParSec = $median
        Essais = $rates
        Echecs = $failures
    }
}

$reference = $results[0].MsgParSec
Write-Host "`n=== Ecart au defaut Kafka ===" -ForegroundColor Cyan
foreach ($result in $results) {
    $facteur = [math]::Round($result.MsgParSec / $reference, 2)
    Write-Host ("  {0,-42} x{1}" -f $result.Scenario, $facteur)
}

Write-Host "`nA lire avec la derniere ligne en tete : si couper la validation change plus que" -ForegroundColor Yellow
Write-Host "tous les reglages reseau reunis, c'est que le facteur limitant n'etait pas le reseau."

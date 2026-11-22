<#
  Enregistrement explicite des schemas dans le Schema Registry (P2-T3, P2-T4).

  WHY un script et non auto.register.schemas=true :
    un schema enregistre par effet de bord au premier demarrage d'un service n'est jamais
    revu. Le premier producteur qui tourne impose le contrat, et une faute de frappe dans un
    nom de champ devient la version 1 du sujet. Ici, publier un contrat est un acte explicite,
    versionne dans git, rejouable.

  Sujets crees, par type d'evenement (TopicRecordNameStrategy : <topic>-<titre du schema>) :
    shop.order.events.v1-OrderPlaced
    shop.order.events.v1.retry-1-OrderPlaced
    shop.order.events.v1.retry-2-OrderPlaced
    shop.order.events.v1.dlt-OrderPlaced
  Les topics derives ont leur propre sujet : le nom du topic fait partie du sujet. Ils
  portent le meme schema, et pourront diverger de politique de compatibilite si besoin.

  Prerequis : infra up (Schema Registry sur http://localhost:8081).
  Usage     : ./infra/schemas/register-schemas.ps1 [-Compatibility BACKWARD]
#>
param(
    [string]$RegistryUrl = "http://localhost:8081",
    [ValidateSet("BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FULL", "NONE")]
    [string]$Compatibility = "BACKWARD",
    [string]$SchemaDir = "common-events/src/main/resources/schemas"
)

$ErrorActionPreference = "Stop"

# WHY cette fonction au lieu de ConvertTo-Json : sur Windows PowerShell 5.1, ConvertTo-Json
# se bloque indefiniment sur une chaine de quelques kilo-octets imbriquee dans un objet
# (@{ schema = $texte } | ConvertTo-Json). La requete n'est jamais emise — le registre ne
# voit rien passer. Echapper a la main est instantane et previsible.
function ConvertTo-JsonStringLiteral {
    param([string]$Text)
    $sb = New-Object System.Text.StringBuilder
    [void]$sb.Append('"')
    foreach ($ch in $Text.ToCharArray()) {
        $code = [int]$ch
        switch ($ch) {
            '"'     { [void]$sb.Append('\"') }
            '\'     { [void]$sb.Append('\\') }
            "`n"    { [void]$sb.Append('\n') }
            "`r"    { [void]$sb.Append('\r') }
            "`t"    { [void]$sb.Append('\t') }
            default {
                if ($code -lt 32 -or $code -gt 126) {
                    [void]$sb.Append(("\u{0:x4}" -f $code))
                } else {
                    [void]$sb.Append($ch)
                }
            }
        }
    }
    [void]$sb.Append('"')
    return $sb.ToString()
}

$contentType = "application/vnd.schemaregistry.v1+json"

$subjectsFile = Join-Path $SchemaDir "subjects.properties"
if (-not (Test-Path $subjectsFile)) { throw "table des sujets absente : $subjectsFile" }

# Suffixes de topic qui portent le meme type d'evenement (cf. CLAUDE.md §2).
$topicSuffixes = @("", ".retry-1", ".retry-2", ".dlt")

$mappings = Get-Content $subjectsFile |
    Where-Object { $_ -match "^\s*[^#\s]" } |
    ForEach-Object {
        $parts = $_ -split "=", 2
        [pscustomobject]@{ EventType = $parts[0].Trim(); Topic = $parts[1].Trim() }
    }

Write-Host "=== Enregistrement, compatibilite $Compatibility ===" -ForegroundColor Cyan

foreach ($mapping in $mappings) {
    $schemaFile = Join-Path $SchemaDir "$($mapping.EventType).json"
    if (-not (Test-Path $schemaFile)) { throw "schema absent : $schemaFile" }
    # WHY -Encoding UTF8 : Get-Content de PowerShell 5.1 lit par defaut dans la page de code
    # ANSI. Un caractere non ASCII du fichier (le "§" des renvois a CLAUDE.md) arrivait
    # double-encode dans le schema publie, sous la forme "ÃÂ§" : le contrat livre aux
    # consumers n'etait alors pas celui du depot.
    $schemaText = Get-Content $schemaFile -Raw -Encoding UTF8

    # Le titre du schema doit etre l'eventType : c'est lui qui forme le nom du sujet.
    $title = ($schemaText | ConvertFrom-Json).title
    if ($title -ne $mapping.EventType) {
        throw "titre '$title' different de l'eventType '$($mapping.EventType)' dans $schemaFile"
    }

    $body = '{"schemaType":"JSON","schema":' + (ConvertTo-JsonStringLiteral $schemaText) + '}'

    foreach ($suffix in $topicSuffixes) {
        $subject = "$($mapping.Topic)$suffix-$($mapping.EventType)"

        # Reenregistrer un schema identique renvoie le meme id : le script est idempotent.
        $registered = Invoke-RestMethod -Method Post -Uri "$RegistryUrl/subjects/$subject/versions" `
            -ContentType $contentType -Body $body -TimeoutSec 30

        Invoke-RestMethod -Method Put -Uri "$RegistryUrl/config/$subject" `
            -ContentType $contentType -TimeoutSec 30 `
            -Body ('{"compatibility":"' + $Compatibility + '"}') | Out-Null

        Write-Host ("  {0,-45} id={1}" -f $subject, $registered.id)
    }
}

Write-Host "`n=== Verification ===" -ForegroundColor Cyan
$subjects = Invoke-RestMethod "$RegistryUrl/subjects" -TimeoutSec 30
Write-Host "sujets presents : $($subjects.Count)"
foreach ($subject in ($subjects | Sort-Object)) {
    $config = Invoke-RestMethod "$RegistryUrl/config/$subject" -TimeoutSec 30
    $versions = Invoke-RestMethod "$RegistryUrl/subjects/$subject/versions" -TimeoutSec 30
    Write-Host ("  {0,-45} compatibilite={1} versions={2}" -f $subject, $config.compatibilityLevel, ($versions -join ","))
}

Write-Host "`nCompatibilite globale du registre : " -NoNewline -ForegroundColor Cyan
(Invoke-RestMethod "$RegistryUrl/config" -TimeoutSec 30).compatibilityLevel

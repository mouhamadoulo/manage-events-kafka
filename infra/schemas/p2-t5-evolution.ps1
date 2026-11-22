<#
  Experience P2-T5 — ce que la compatibilite BACKWARD autorise, et ce qu'elle refuse.

  BACKWARD se lit dans un seul sens : un consumer equipe du NOUVEAU schema doit pouvoir lire
  les ANCIENS messages. C'est l'ordre de deploiement implicite : consumers d'abord,
  producteurs ensuite.

  Trois variantes sont soumises au registre, sans rien casser : tout se passe sur un sujet de
  laboratoire (lab.evolution-OrderPlaced), supprime a la fin. Le contrat reel reste en v1.

    A  champ optionnel ajoute   -> les vieux messages restent lisibles
    B  champ requis ajoute      -> les vieux messages n'ont pas le champ
    C  champ retire             -> depend du modele de contenu (additionalProperties)

  Prerequis : infra up.
  Usage     : ./infra/schemas/p2-t5-evolution.ps1
#>
param(
    [string]$RegistryUrl = "http://localhost:8081",
    [string]$Subject = "lab.evolution-OrderPlaced",
    [string]$BaseSchema = "common-events/src/main/resources/schemas/OrderPlaced.json",
    [string]$VariantDir = "infra/schemas/experiments"
)

$ErrorActionPreference = "Stop"
$contentType = "application/vnd.schemaregistry.v1+json"

# WHY: ConvertTo-Json se bloque sur une chaine de quelques kilo-octets imbriquee dans un
# objet, sur Windows PowerShell 5.1 (cf. register-schemas.ps1).
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
                if ($code -lt 32 -or $code -gt 126) { [void]$sb.Append(("\u{0:x4}" -f $code)) }
                else { [void]$sb.Append($ch) }
            }
        }
    }
    [void]$sb.Append('"')
    return $sb.ToString()
}

# WHY -Encoding UTF8 : Get-Content de PowerShell 5.1 lit par defaut dans la page de code
# ANSI. Un caractere non ASCII du fichier (un "§" de reference a CLAUDE.md) arrivait
# double-encode dans le schema publie : "ÃÂ§". Le contrat livre aux consumers n'etait pas
# celui du depot.
function New-SchemaBody {
    param([string]$Path)
    return '{"schemaType":"JSON","schema":' + (ConvertTo-JsonStringLiteral (Get-Content $Path -Raw -Encoding UTF8)) + '}'
}

function Test-Compatibility {
    param([string]$Label, [string]$Path)

    Write-Host "`n--- $Label ---" -ForegroundColor Cyan
    # verbose=true : le registre explique POURQUOI il refuse. Sans ce parametre il repond
    # juste false, ce qui n'aide personne.
    $result = Invoke-RestMethod -Method Post -TimeoutSec 30 `
        -Uri "$RegistryUrl/compatibility/subjects/$Subject/versions/latest?verbose=true" `
        -ContentType $contentType -Body (New-SchemaBody $Path)

    if ($result.is_compatible) {
        Write-Host "  COMPATIBLE" -ForegroundColor Green
    } else {
        Write-Host "  REFUSE" -ForegroundColor Yellow
        foreach ($message in $result.messages) { Write-Host "    $message" }
    }
    return $result.is_compatible
}

function Remove-LabSubject {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        Invoke-RestMethod -Method Delete -TimeoutSec 15 "$RegistryUrl/subjects/$Subject" | Out-Null
        Invoke-RestMethod -Method Delete -TimeoutSec 15 "$($RegistryUrl)/subjects/$($Subject)?permanent=true" | Out-Null
    } catch { }
    $ErrorActionPreference = $previous
}

try {
    Remove-LabSubject

    Write-Host "=== Sujet de laboratoire : version 1 = le contrat actuel ===" -ForegroundColor Cyan
    $v1 = Invoke-RestMethod -Method Post -TimeoutSec 30 `
        -Uri "$RegistryUrl/subjects/$Subject/versions" `
        -ContentType $contentType -Body (New-SchemaBody $BaseSchema)
    Invoke-RestMethod -Method Put -TimeoutSec 30 -Uri "$RegistryUrl/config/$Subject" `
        -ContentType $contentType -Body '{"compatibility":"BACKWARD"}' | Out-Null
    Write-Host "  version 1 enregistree, id=$($v1.id), compatibilite=BACKWARD"

    $okA = Test-Compatibility "A : ajout de 'channel', OPTIONNEL" "$VariantDir/OrderPlaced-A-champ-optionnel.json"
    $okB = Test-Compatibility "B : ajout de 'channel', OBLIGATOIRE" "$VariantDir/OrderPlaced-B-champ-requis.json"
    $okC = Test-Compatibility "C : retrait de 'currency'" "$VariantDir/OrderPlaced-C-champ-retire.json"

    if ($okA) {
        Write-Host "`n=== La variante A est donc deployable : on l'enregistre ===" -ForegroundColor Cyan
        $v2 = Invoke-RestMethod -Method Post -TimeoutSec 30 `
            -Uri "$RegistryUrl/subjects/$Subject/versions" `
            -ContentType $contentType -Body (New-SchemaBody "$VariantDir/OrderPlaced-A-champ-optionnel.json")
        Write-Host "  version 2 enregistree, id=$($v2.id)"
        $versions = Invoke-RestMethod -TimeoutSec 30 "$RegistryUrl/subjects/$Subject/versions"
        Write-Host "  versions du sujet : $($versions -join ', ')"
        Write-Host "  Les messages v1 deja dans le topic restent lisibles : BACKWARD garantit"
        Write-Host "  cela. La reciproque est fausse : un consumer reste en v1 qui recoit un"
        Write-Host "  message v2 voit un champ inconnu. De la vient le reglage"
        Write-Host "  json.fail.unknown.properties=false cote consumer."
    }

    Write-Host "`n=== Bilan ===" -ForegroundColor Cyan
    Write-Host ("  A  champ optionnel  : {0}" -f ($(if ($okA) { "compatible" } else { "refuse" })))
    Write-Host ("  B  champ requis     : {0}" -f ($(if ($okB) { "compatible" } else { "refuse" })))
    Write-Host ("  C  champ retire     : {0}" -f ($(if ($okC) { "compatible" } else { "refuse" })))
}
finally {
    Remove-LabSubject
    Write-Host "`nsujet de laboratoire supprime, le contrat reel est intact." -ForegroundColor Green
}

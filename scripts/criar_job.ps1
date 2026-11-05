# Cria ou atualiza (idempotente por nome) o Job "ANTT Lakehouse - mensal" no Databricks Free Edition.
# Le scripts\job_antt_lakehouse.json, substitui o placeholder __WS_BASE__ pelo caminho do usuario logado
# (/Users/<userName>/antt-lakehouse) e so entao cria/reseta o job. O arquivo versionado nao e alterado.
# Uso: powershell -NoProfile -ExecutionPolicy Bypass -File scripts\criar_job.ps1 [-Perfil antt] [-PastaWs ...]
param([string]$Perfil = "antt", [string]$PastaWs = "")
$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$utf8 = New-Object System.Text.UTF8Encoding($false)
. "$PSScriptRoot\_comum.ps1"

$raiz = Get-RaizProjeto
$dbx = Get-DatabricksCli
$ws = Get-WsBase -Dbx $dbx -Perfil $Perfil -PastaWs $PastaWs
$nomeJob = "ANTT Lakehouse - mensal"
$defFile = "$raiz\scripts\job_antt_lakehouse.json"
$dataDir = "$raiz\data"

Write-Host "Usuario/pasta no workspace: $ws (perfil $Perfil)"

# Aviso opcional: a pasta de notebooks existe no workspace?
$existeWs = & $dbx workspace list $ws --profile $Perfil 2>&1 | Out-String
if ($LASTEXITCODE -ne 0 -or -not $existeWs.Trim()) {
  Write-Host "AVISO: $ws parece vazia ou inexistente. Importe os notebooks antes (scripts\importar_notebooks.ps1)."
}

# Resolve o placeholder __WS_BASE__ em memoria e grava a definicao efetiva num arquivo temporario em data\.
$defResolvida = (Get-Content $defFile -Raw) -replace '__WS_BASE__', $ws
$defTmp = "$dataDir\_job_def.json"
[IO.File]::WriteAllText($defTmp, $defResolvida, $utf8)

# 1) Procura job existente pelo nome
$listJson = & $dbx jobs list --profile $Perfil -o json 2>&1 | Out-String
try { $lista = $listJson | ConvertFrom-Json } catch { $lista = @() }
if ($lista -and $lista.PSObject.Properties.Name -contains "jobs") { $lista = $lista.jobs }
$existente = $lista | Where-Object { $_.settings.name -eq $nomeJob } | Select-Object -First 1

if ($existente) {
  $jobId = $existente.job_id
  Write-Host "Job ja existe (job_id=$jobId). Fazendo reset..."
  $settings = Get-Content $defTmp -Raw | ConvertFrom-Json
  $payload = @{ job_id = $jobId; new_settings = $settings } | ConvertTo-Json -Depth 20
  $pf = "$dataDir\_job_reset.json"
  [IO.File]::WriteAllText($pf, $payload, $utf8)
  & $dbx jobs reset --json "@$pf" --profile $Perfil 2>&1 | Out-String | Write-Host
  Write-Host "job_id=$jobId"
}
else {
  Write-Host "Job nao existe. Criando..."
  $out = & $dbx jobs create --json "@$defTmp" --profile $Perfil -o json 2>&1 | Out-String
  Write-Host $out
  try { $jobId = ($out | ConvertFrom-Json).job_id } catch { $jobId = $null }
  Write-Host "job_id=$jobId"
}

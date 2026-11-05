# Cria ou atualiza (idempotente por nome) o Job "ANTT Lakehouse - mensal" no Databricks Free Edition.
# Se o job ja existe, faz reset com a definicao de scripts\job_antt_lakehouse.json; senao, cria.
# Uso: powershell -NoProfile -ExecutionPolicy Bypass -File scripts\criar_job.ps1
$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$utf8 = New-Object System.Text.UTF8Encoding($false)

$raiz = "C:\Users\uiliam.bittencourt\Documents\dev-out"
$dbx = "$env:LOCALAPPDATA\databricks-cli\databricks.exe"
$profile = "antt"
$nomeJob = "ANTT Lakehouse - mensal"
$defFile = "$raiz\scripts\job_antt_lakehouse.json"
$dataDir = "$raiz\data"

# 1) Procura job existente pelo nome
$listJson = & $dbx jobs list --profile $profile -o json 2>&1 | Out-String
try { $lista = $listJson | ConvertFrom-Json } catch { $lista = @() }
if ($lista -and $lista.PSObject.Properties.Name -contains "jobs") { $lista = $lista.jobs }
$existente = $lista | Where-Object { $_.settings.name -eq $nomeJob } | Select-Object -First 1

if ($existente) {
  $jobId = $existente.job_id
  Write-Host "Job ja existe (job_id=$jobId). Fazendo reset..."
  $settings = Get-Content $defFile -Raw | ConvertFrom-Json
  $payload = @{ job_id = $jobId; new_settings = $settings } | ConvertTo-Json -Depth 20
  $pf = "$dataDir\_job_reset.json"
  [IO.File]::WriteAllText($pf, $payload, $utf8)
  & $dbx jobs reset --json "@$pf" --profile $profile 2>&1 | Out-String | Write-Host
  Write-Host "job_id=$jobId"
}
else {
  Write-Host "Job nao existe. Criando..."
  $out = & $dbx jobs create --json "@$defFile" --profile $profile -o json 2>&1 | Out-String
  Write-Host $out
  try { $jobId = ($out | ConvertFrom-Json).job_id } catch { $jobId = $null }
  Write-Host "job_id=$jobId"
}

# Copia notebooks\08_analise.sql para o Volume e executa o notebook 95_verificacao_consultas, que roda cada consulta.
# Uso: powershell -NoProfile -ExecutionPolicy Bypass -File scripts\validar_consultas.ps1 -Tag q1 [-Perfil antt] [-PastaWs ...]
param([string]$Tag = "q1", [string]$Perfil = "antt", [string]$PastaWs = "")
$ErrorActionPreference = "Stop"
. "$PSScriptRoot\_comum.ps1"
$raiz = Get-RaizProjeto
$dbx = Get-DatabricksCli
$utf8 = New-Object System.Text.UTF8Encoding($false)
$log = "$raiz\data\_cp_$Tag.txt"
$r = & $dbx fs cp "$raiz\notebooks\08_analise.sql" "dbfs:/Volumes/workspace/antt_raw/landing/_resultados/08_analise.sql" --overwrite --profile $Perfil 2>&1 | Out-String
[IO.File]::WriteAllText($log, "cp: $r`r`nCOPIADO`r`n", $utf8)
& "$PSScriptRoot\executar_notebook_databricks.ps1" -Nb "95_verificacao_consultas" -Tag $Tag -Perfil $Perfil -PastaWs $PastaWs

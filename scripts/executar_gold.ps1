# Importa os notebooks da Gold (dimensoes, fatos, verificacao) no workspace e executa o orquestrador 05_gold_run_all.
# Uso: powershell -NoProfile -ExecutionPolicy Bypass -File scripts\executar_gold.ps1 -Tag gld1 [-Perfil antt] [-PastaWs ...]
param([string]$Tag = "gld1", [string]$Perfil = "antt", [string]$PastaWs = "")
$ErrorActionPreference = "Stop"
. "$PSScriptRoot\_comum.ps1"
$raiz = Get-RaizProjeto
$dbx = Get-DatabricksCli
$ws = Get-WsBase -Dbx $dbx -Perfil $Perfil -PastaWs $PastaWs
$utf8 = New-Object System.Text.UTF8Encoding($false)
$log = "$raiz\data\_imp_$Tag.txt"
[IO.File]::WriteAllText($log, "inicio $(Get-Date -Format s)`r`nws=$ws perfil=$Perfil`r`n", $utf8)
& $dbx workspace mkdirs $ws --profile $Perfil 2>&1 | Out-Null
foreach ($n in @("05_gold_dimensoes", "05_gold_fatos", "95_verificacao_gold")) {
  $r = & $dbx workspace import "$ws/$n" --file "$raiz\notebooks\$n.py" --language PYTHON --format SOURCE --overwrite --profile $Perfil 2>&1 | Out-String
  [IO.File]::AppendAllText($log, "import $n : $r`r`n", $utf8)
}
[IO.File]::AppendAllText($log, "IMPORTADO`r`n", $utf8)
& "$PSScriptRoot\executar_notebook_databricks.ps1" -Nb "05_gold_run_all" -Tag $Tag -Perfil $Perfil -PastaWs $PastaWs

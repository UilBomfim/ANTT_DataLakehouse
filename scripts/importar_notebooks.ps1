# Importa TODOS os notebooks de notebooks\ (.py como Python, .sql como SQL) para o workspace e lista o que ficou la.
# Descobre o usuario logado automaticamente (databricks current-user me) e cria a pasta /Users/<voce>/antt-lakehouse.
# Uso: powershell -NoProfile -ExecutionPolicy Bypass -File scripts\importar_notebooks.ps1 [-Perfil antt] [-PastaWs /Users/.../antt-lakehouse]
param([string]$Perfil = "antt", [string]$PastaWs = "")
$ErrorActionPreference = "Stop"
. "$PSScriptRoot\_comum.ps1"
$raiz = Get-RaizProjeto
$dbx = Get-DatabricksCli
$ws = Get-WsBase -Dbx $dbx -Perfil $Perfil -PastaWs $PastaWs
$utf8 = New-Object System.Text.UTF8Encoding($false)
$log = "$raiz\data\_importar.txt"
[IO.File]::WriteAllText($log, "inicio $(Get-Date -Format s)`r`nws=$ws perfil=$Perfil`r`n", $utf8)
& $dbx workspace mkdirs $ws --profile $Perfil 2>&1 | Out-Null
foreach ($f in Get-ChildItem "$raiz\notebooks" -File | Where-Object { $_.Extension -in ".py", ".sql" }) {
  $lang = if ($f.Extension -eq ".sql") { "SQL" } else { "PYTHON" }
  $nome = [IO.Path]::GetFileNameWithoutExtension($f.Name)
  $r = & $dbx workspace import "$ws/$nome" --file $f.FullName --language $lang --format SOURCE --overwrite --profile $Perfil 2>&1 | Out-String
  [IO.File]::AppendAllText($log, "$nome ($lang): " + $(if ($r.Trim()) { $r.Trim() } else { "ok" }) + "`r`n", $utf8)
}
[IO.File]::AppendAllText($log, "--- no workspace ---`r`n", $utf8)
$l = & $dbx workspace list $ws --profile $Perfil 2>&1 | Out-String
[IO.File]::AppendAllText($log, $l + "`r`nFIM`r`n", $utf8)

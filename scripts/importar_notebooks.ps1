# Importa TODOS os notebooks de notebooks\ (.py como Python, .sql como SQL) para o workspace e lista o que ficou la.
# Uso: powershell -NoProfile -ExecutionPolicy Bypass -File scripts\importar_notebooks.ps1
$raiz = "C:\Users\uiliam.bittencourt\Documents\dev-out"
$dbx = "$env:LOCALAPPDATA\databricks-cli\databricks.exe"
$ws = "/Users/uilian.senna@gmail.com/antt-lakehouse"
$utf8 = New-Object System.Text.UTF8Encoding($false)
$log = "$raiz\data\_importar.txt"
[IO.File]::WriteAllText($log, "inicio $(Get-Date -Format s)`r`n", $utf8)
foreach ($f in Get-ChildItem "$raiz\notebooks" -File | Where-Object { $_.Extension -in ".py", ".sql" }) {
  $lang = if ($f.Extension -eq ".sql") { "SQL" } else { "PYTHON" }
  $nome = [IO.Path]::GetFileNameWithoutExtension($f.Name)
  $r = & $dbx workspace import "$ws/$nome" --file $f.FullName --language $lang --format SOURCE --overwrite --profile antt 2>&1 | Out-String
  [IO.File]::AppendAllText($log, "$nome ($lang): " + $(if ($r.Trim()) { $r.Trim() } else { "ok" }) + "`r`n", $utf8)
}
[IO.File]::AppendAllText($log, "--- no workspace ---`r`n", $utf8)
$l = & $dbx workspace list $ws --profile antt 2>&1 | Out-String
[IO.File]::AppendAllText($log, $l + "`r`nFIM`r`n", $utf8)

# Importa os notebooks de indicadores da Gold no workspace e executa o orquestrador 06_gold_run_all.
# Uso: powershell -NoProfile -ExecutionPolicy Bypass -File scripts\executar_gold_indicadores.ps1 -Tag ind1
param([string]$Tag = "ind1")
$raiz = "C:\Users\uiliam.bittencourt\Documents\dev-out"
$dbx = "$env:LOCALAPPDATA\databricks-cli\databricks.exe"
$ws = "/Users/uilian.senna@gmail.com/antt-lakehouse"
$utf8 = New-Object System.Text.UTF8Encoding($false)
$log = "$raiz\data\_imp_$Tag.txt"
[IO.File]::WriteAllText($log, "inicio $(Get-Date -Format s)`r`n", $utf8)
foreach ($n in @("06_gold_indicadores", "95_verificacao_gold_agg")) {
  $r = & $dbx workspace import "$ws/$n" --file "$raiz\notebooks\$n.py" --language PYTHON --format SOURCE --overwrite --profile antt 2>&1 | Out-String
  [IO.File]::AppendAllText($log, "import $n : $r`r`n", $utf8)
}
[IO.File]::AppendAllText($log, "IMPORTADO`r`n", $utf8)
& "$raiz\scripts\executar_notebook_databricks.ps1" -Nb "06_gold_run_all" -Tag $Tag

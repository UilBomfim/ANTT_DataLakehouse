# Importa os notebooks da Gold (dimensoes, fatos, verificacao) no workspace e executa o orquestrador 05_gold_run_all.
# Uso: powershell -NoProfile -ExecutionPolicy Bypass -File scripts\executar_gold.ps1 -Tag gld1
param([string]$Tag = "gld1")
$raiz = "C:\Users\uiliam.bittencourt\Documents\dev-out"
$dbx = "$env:LOCALAPPDATA\databricks-cli\databricks.exe"
$ws = "/Users/uilian.senna@gmail.com/antt-lakehouse"
$utf8 = New-Object System.Text.UTF8Encoding($false)
$log = "$raiz\data\_imp_$Tag.txt"
[IO.File]::WriteAllText($log, "inicio $(Get-Date -Format s)`r`n", $utf8)
foreach ($n in @("05_gold_dimensoes", "05_gold_fatos", "95_verificacao_gold")) {
  $r = & $dbx workspace import "$ws/$n" --file "$raiz\notebooks\$n.py" --language PYTHON --format SOURCE --overwrite --profile antt 2>&1 | Out-String
  [IO.File]::AppendAllText($log, "import $n : $r`r`n", $utf8)
}
[IO.File]::AppendAllText($log, "IMPORTADO`r`n", $utf8)
& "$raiz\scripts\executar_notebook_databricks.ps1" -Nb "05_gold_run_all" -Tag $Tag

# Copia notebooks\08_analise.sql para o Volume e executa o notebook 95_verificacao_consultas, que roda cada consulta.
# Uso: powershell -NoProfile -ExecutionPolicy Bypass -File scripts\validar_consultas.ps1 -Tag q1
param([string]$Tag = "q1")
$raiz = "C:\Users\uiliam.bittencourt\Documents\dev-out"
$dbx = "$env:LOCALAPPDATA\databricks-cli\databricks.exe"
$utf8 = New-Object System.Text.UTF8Encoding($false)
$log = "$raiz\data\_cp_$Tag.txt"
$r = & $dbx fs cp "$raiz\notebooks\08_analise.sql" "dbfs:/Volumes/workspace/antt_raw/landing/_resultados/08_analise.sql" --overwrite --profile antt 2>&1 | Out-String
[IO.File]::WriteAllText($log, "cp: $r`r`nCOPIADO`r`n", $utf8)
& "$raiz\scripts\executar_notebook_databricks.ps1" -Nb "95_verificacao_consultas" -Tag $Tag

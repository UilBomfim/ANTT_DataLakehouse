# Importa UM notebook de notebooks\ (.py como Python, .sql como SQL) e confere o idioma no workspace.
# Uso: powershell -NoProfile -ExecutionPolicy Bypass -File scripts\importar_um_notebook.ps1 -Arquivo 08_analise.sql
param([string]$Arquivo)
$raiz = "C:\Users\uiliam.bittencourt\Documents\dev-out"
$dbx = "$env:LOCALAPPDATA\databricks-cli\databricks.exe"
$ws = "/Users/uilian.senna@gmail.com/antt-lakehouse"
$utf8 = New-Object System.Text.UTF8Encoding($false)
$log = "$raiz\data\_um_$([IO.Path]::GetFileNameWithoutExtension($Arquivo))_$(Get-Date -Format HHmmss).txt"
[IO.File]::WriteAllText($log, "inicio $(Get-Date -Format s)`r`n", $utf8)
$f = Get-Item "$raiz\notebooks\$Arquivo"
$lang = if ($f.Extension -eq ".sql") { "SQL" } else { "PYTHON" }
$nome = [IO.Path]::GetFileNameWithoutExtension($f.Name)
$r = & $dbx workspace import "$ws/$nome" --file $f.FullName --language $lang --format SOURCE --overwrite --profile antt 2>&1 | Out-String
[IO.File]::AppendAllText($log, "import $nome ($lang): " + $(if ($r.Trim()) { $r.Trim() } else { "ok" }) + "`r`n", $utf8)
$e = & $dbx workspace export "$ws/$nome" --format SOURCE --profile antt 2>&1 | Out-String
$linhas = ($e -split "`n").Count
$ok = ($e -match "Gloss") -and ($e -match "localidade_key") -and ($e -match "agg_evolucao_mensal_volume")
[IO.File]::AppendAllText($log, "export: $linhas linhas; contem glossario: $ok`r`nFIM`r`n", $utf8)

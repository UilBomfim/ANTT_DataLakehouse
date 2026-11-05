param([string]$Nb, [string]$Tag, [string]$Params = "{}")
$ErrorActionPreference = "Continue"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$dbx = "$env:LOCALAPPDATA\databricks-cli\databricks.exe"
$dataDir = "C:\Users\uiliam.bittencourt\Documents\dev-out\data"
$log = "$dataDir\_log_$Tag.txt"
$res = "$dataDir\_res_$Tag.json"
$wsDir = "/Users/uilian.senna@gmail.com/antt-lakehouse"
$utf8 = New-Object System.Text.UTF8Encoding($false)
function L($m) { [IO.File]::AppendAllText($log, "$(Get-Date -Format s) $m`r`n", $utf8) }
[IO.File]::WriteAllText($log, "", $utf8)

& $dbx workspace mkdirs $wsDir --profile antt 2>&1 | Out-Null
$imp = & $dbx workspace import "$wsDir/$Nb" --file "C:\Users\uiliam.bittencourt\Documents\dev-out\notebooks\$Nb.py" --language PYTHON --format SOURCE --overwrite --profile antt 2>&1 | Out-String
L "import: $imp"

$bp = @{}
(ConvertFrom-Json $Params).PSObject.Properties | ForEach-Object { $bp[$_.Name] = [string]$_.Value }
$payload = @{
  run_name = "antt-$Tag"
  tasks = @(@{ task_key = "t"; notebook_task = @{ notebook_path = "$wsDir/$Nb"; source = "WORKSPACE"; base_parameters = $bp } })
} | ConvertTo-Json -Depth 8
$pf = "$dataDir\_payload_$Tag.json"
[IO.File]::WriteAllText($pf, $payload, $utf8)
$sub = & $dbx jobs submit --json "@$pf" --no-wait --profile antt -o json 2>&1 | Out-String
L "submit: $sub"
try { $runId = ($sub | ConvertFrom-Json).run_id } catch { L "falha ao ler run_id"; L "FIM"; return }
L "run_id=$runId"

for ($i = 0; $i -lt 360; $i++) {
  $j = & $dbx jobs get-run $runId --profile antt -o json 2>&1 | Out-String
  try { $run = $j | ConvertFrom-Json } catch { Start-Sleep 20; continue }
  $life = $run.state.life_cycle_state
  L "estado=$life $($run.state.result_state)"
  if ($life -in @("TERMINATED", "INTERNAL_ERROR", "SKIPPED")) {
    $tid = $run.tasks[0].run_id
    $o = & $dbx jobs get-run-output $tid --profile antt -o json 2>&1 | Out-String
    try { $oj = $o | ConvertFrom-Json; $o = ($oj | Select-Object notebook_output, error, error_trace | ConvertTo-Json -Depth 6) } catch { }
    [IO.File]::WriteAllText($res, $o, $utf8)
    L "saida gravada em $res"
    break
  }
  Start-Sleep 20
}
L "FIM"

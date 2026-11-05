# Investiga no CKAN da ANTT os periodos faltantes: viagens/paradas fev/2026, empresas regular mar/2026, paradas ago/2026.
$base = "https://dados.antt.gov.br/api/3/action"
$saida = "c:\Users\uiliam.bittencourt\Documents\dev-out\data\_periodos_faltantes.txt"
"" | Out-File $saida -Encoding utf8
function Log($t) { $t | Out-File $saida -Append -Encoding utf8 }

function Get-Json($url) {
  $tmp = [System.IO.Path]::GetTempFileName()
  $code = curl.exe -s --ssl-no-revoke -m 90 -o $tmp -w "%{http_code}" $url
  if ($code -ne "200") { return $null }
  Get-Content $tmp -Raw -Encoding utf8 | ConvertFrom-Json
}

$pacotes = @{ viagens = "monitriip-servico-fretado-viagens"; paradas = "monitriip-servico-fretado-paradas"; empresas = "empresas-habilitadas" }
foreach ($k in $pacotes.Keys) {
  $p = Get-Json "$base/package_show?id=$($pacotes[$k])"
  if (-not $p) { Log "[$k] package_show falhou"; continue }
  $r = $p.result
  Log "== $k | metadata_modified=$($r.metadata_modified) | resources=$($r.resources.Count) | num_resources=$($r.num_resources)"
  $alvo = $r.resources | Where-Object { $_.name -match "Fev26|Mar26|Ago26|Jan26" -and $_.format -in @("CSV","JSON") }
  $alvo | ForEach-Object { Log ("   {0} | {1} | size={2} | mod={3} | state={4} | {5}" -f $_.name, $_.format, $_.size, $_.last_modified, $_.state, $_.url) }
}

# Historico de atividade (mostra recursos removidos/alterados)
foreach ($k in @("viagens", "paradas", "empresas")) {
  $a = Get-Json "$base/package_activity_list?id=$($pacotes[$k])&limit=100"
  if (-not $a) { Log "[$k] package_activity_list indisponivel"; continue }
  Log "== atividade $k : $($a.result.Count) eventos"
  $a.result | Select-Object -First 40 | ForEach-Object { Log ("   {0} | {1}" -f $_.timestamp, $_.activity_type) }
}

# Busca de recursos por nome em todo o portal
foreach ($q in @("name:Fev26", "name:viagem_fretado_02_2026", "url:parada_fretado_02_2026")) {
  $s = Get-Json "$base/resource_search?query=$([uri]::EscapeDataString($q))&limit=20"
  if ($s) { Log "== resource_search '$q' : count=$($s.result.count)"; $s.result.results | Select-Object -First 8 | ForEach-Object { Log ("   {0} | {1} | {2}" -f $_.name, $_.format, $_.url) } } else { Log "== resource_search '$q' indisponivel" }
}

# Teste direto de URLs prováveis (HEAD) para fevereiro: nomes de arquivo seguem o padrão
$tests = @(
  "https://dados.antt.gov.br/dataset/8ddd197d-81e6-43bb-a2b3-c9a158ddfbae/resource/*/download/viagem_fretado_02_2026.csv"
)
Log "== FIM"

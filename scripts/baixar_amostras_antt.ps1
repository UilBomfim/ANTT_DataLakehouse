# Baixa 1 CSV de amostra por conjunto (mais recente) para data\amostras\<conjunto>\ e mostra cabecalho/tamanho.
$raiz = "c:\Users\uiliam.bittencourt\Documents\dev-out"
$csv = Import-Csv "$raiz\data\recursos_antt.csv"
$saida = "$raiz\data\resumo_amostras.txt"
"" | Out-File $saida -Encoding utf8

$escolhas = @(
  @{ pasta = "viagens_fretado";  conjunto = "monitriip-servico-fretado-viagens";        filtro = { $_.formato -eq "CSV" } },
  @{ pasta = "paradas_fretado";  conjunto = "monitriip-servico-fretado-paradas";        filtro = { $_.formato -eq "CSV" } },
  @{ pasta = "bilhetes";         conjunto = "monitriip-bilhetes-de-passagem";           filtro = { $_.formato -eq "CSV" } },
  @{ pasta = "empresas";         conjunto = "empresas-habilitadas";                     filtro = { $_.formato -eq "CSV" -and $_.recurso -notmatch "Semiurbano|TRIC|Multimodal|Peru" } },
  @{ pasta = "licencas_continuo"; conjunto = "autorizacoes-de-viagem-fretado-continuo"; filtro = { $_.formato -eq "CSV" } },
  @{ pasta = "sisaut";           conjunto = "sisaut-nacional-internacional";            filtro = { $_.formato -eq "CSV" } }
)

foreach ($e in $escolhas) {
  $r = $csv | Where-Object { $_.conjunto -eq $e.conjunto } | Where-Object $e.filtro | Sort-Object modificado | Select-Object -Last 1
  if (-not $r) { "== $($e.pasta): nenhum recurso" | Out-File $saida -Append -Encoding utf8; continue }
  $dir = "$raiz\data\amostras\$($e.pasta)"
  New-Item -ItemType Directory -Force -Path $dir | Out-Null
  $arq = Join-Path $dir ([System.IO.Path]::GetFileName(([uri]$r.url).AbsolutePath))
  $code = curl.exe -s -L --ssl-no-revoke -m 900 -o $arq -w "%{http_code}" $r.url
  $tam = if (Test-Path $arq) { (Get-Item $arq).Length } else { 0 }
  "== $($e.pasta) | $($r.recurso) | modificado=$($r.modificado) | HTTP $code | bytes=$tam" | Out-File $saida -Append -Encoding utf8
  if ($tam -gt 0) {
    $linhas = [System.IO.File]::ReadLines($arq) | Select-Object -First 3
    $linhas | ForEach-Object { "   " + $_.Substring(0, [Math]::Min(600, $_.Length)) } | Out-File $saida -Append -Encoding utf8
  }
}
"FIM" | Out-File $saida -Append -Encoding utf8

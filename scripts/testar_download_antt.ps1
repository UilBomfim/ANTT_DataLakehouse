# Lista os conjuntos restantes e testa o download (HEAD) do arquivo CSV mais recente de cada conjunto.
$ErrorActionPreference = "Continue"
$raiz = "c:\Users\uiliam.bittencourt\Documents\dev-out"
$base = "https://dados.antt.gov.br/api/3/action"
$saida = "$raiz\data\resumo_download.txt"
"" | Out-File $saida -Encoding utf8

function Get-Json($url) {
  $tmp = [System.IO.Path]::GetTempFileName()
  $code = curl.exe -s --ssl-no-revoke -m 90 -o $tmp -w "%{http_code}" $url
  if ($code -ne "200") { throw "HTTP $code em $url" }
  Get-Content $tmp -Raw -Encoding utf8 | ConvertFrom-Json
}

$slugs = @("monitriip-servico-fretado-viagens", "monitriip-servico-fretado-paradas", "monitriip-bilhetes-de-passagem",
           "empresas-habilitadas", "autorizacoes-de-viagem-fretado-continuo", "sisaut-nacional-internacional")
$todos = @()
foreach ($s in $slugs) {
  try { $p = (Get-Json "$base/package_show?id=$s").result } catch { "ERRO $s : $_" | Out-File $saida -Append -Encoding utf8; continue }
  $recs = $p.resources | ForEach-Object {
    [pscustomobject]@{ conjunto = $p.name; recurso = $_.name; formato = $_.format; tamanho = $_.size; modificado = $_.last_modified; url = $_.url }
  }
  $todos += $recs
  $dados = $recs | Where-Object { $_.formato -match 'CSV|ZIP|XLS|JSON' }
  "== $s | total=$($recs.Count) | dados=$($dados.Count) | formatos: " + (($recs | Group-Object formato | ForEach-Object { "$($_.Name)=$($_.Count)" }) -join ", ") | Out-File $saida -Append -Encoding utf8
  $ult = $dados | Sort-Object modificado | Select-Object -Last 1
  if ($ult) {
    $hdr = curl.exe -s -I -L --ssl-no-revoke -m 60 $ult.url 2>&1 | Out-String
    $status = ([regex]::Matches($hdr, 'HTTP/\S+ (\d+)') | ForEach-Object { $_.Groups[1].Value }) -join ">"
    $len = ([regex]::Matches($hdr, '(?im)^content-length:\s*(\d+)') | Select-Object -Last 1).Groups[1].Value
    "   ultimo: $($ult.recurso) | $($ult.modificado) | status=$status | bytes=$len" | Out-File $saida -Append -Encoding utf8
    "   url: $($ult.url)" | Out-File $saida -Append -Encoding utf8
  }
}
$todos | Export-Csv "$raiz\data\recursos_antt.csv" -NoTypeInformation -Encoding utf8
"FIM" | Out-File $saida -Append -Encoding utf8

$ErrorActionPreference = "Continue"
$raiz = "C:\Users\uiliam.bittencourt\Documents\dev-out"
Set-Location $raiz
$log = "$raiz\data\_push.txt"
"inicio $(Get-Date -Format s)" | Out-File $log -Encoding utf8

# Garante identidade local (so para este repo)
git config user.name  "Uilian Bomfim"      2>&1 | Out-File $log -Append -Encoding utf8
git config user.email "uilian.senna@gmail.com" 2>&1 | Out-File $log -Append -Encoding utf8

# Remote (idempotente)
git remote remove origin 2>$null
git remote add origin https://github.com/UilBomfim/ANTT_DataLakehouse.git 2>&1 | Out-File $log -Append -Encoding utf8

# Commit se houver algo para commitar
git add -A 2>&1 | Out-File $log -Append -Encoding utf8
"-- status --" | Out-File $log -Append -Encoding utf8
git status -s 2>&1 | Out-File $log -Append -Encoding utf8
$pendente = git status --porcelain
if ($pendente) {
  git commit -m "ANTT Lakehouse: pipeline Bronze/Silver/Gold, Job mensal, analises e docs" 2>&1 | Out-File $log -Append -Encoding utf8
} else {
  "nada novo para commitar (commit ja existe)" | Out-File $log -Append -Encoding utf8
}

git branch -M main 2>&1 | Out-File $log -Append -Encoding utf8
"-- log --" | Out-File $log -Append -Encoding utf8
git log --oneline -3 2>&1 | Out-File $log -Append -Encoding utf8

"-- push --" | Out-File $log -Append -Encoding utf8
git push -u origin main 2>&1 | Out-File $log -Append -Encoding utf8
"FIM rc=$LASTEXITCODE" | Out-File $log -Append -Encoding utf8
